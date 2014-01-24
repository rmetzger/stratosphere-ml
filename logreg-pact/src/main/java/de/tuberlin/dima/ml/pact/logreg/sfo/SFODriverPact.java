package de.tuberlin.dima.ml.pact.logreg.sfo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import de.tuberlin.dima.ml.logreg.sfo.FeatureGain;
import de.tuberlin.dima.ml.logreg.sfo.IncrementalModel;
import de.tuberlin.dima.ml.logreg.sfo.SFODriver;
import de.tuberlin.dima.ml.pact.JobRunner;
import eu.stratosphere.api.common.Plan;

/**
 * Implementation of SFO [1] for Stratosphere / PACT
 * 
 * @See {@link SFODriver} for a short documentation of SFS
 * 
 * [1] Singh, S., Kubica, J., Larsen, S., & Sorokina, D. (2009).
 * Parallel Large Scale Feature Selection for Logistic Regression. Optimization,
 * 1172–1183.
 */
public class SFODriverPact implements SFODriver {

  private String inputPathTrain;
  private String inputPathTest;
  private boolean isMultilabelInput;
  private int positiveClass;
  private String outputPath;
  private int highestFeatureId;
  private double newtonTolerance;
  private int newtonMaxIterations;
  private double regularization;
  private boolean runLocal;
  private String confPath;
  private String jarPath;

  private IncrementalModel baseModel;
  private List<FeatureGain> gains = Lists.newArrayList();
  
  private Map<String, Long> counters = Maps.newHashMap();
  public static final String COUNTER_KEY_TOTAL_WALLCLOCK = "total-wall-clock";
  private static final String COUNTER_KEY_READ_RESULT = "read-result-gains-and-coefficients";
  
  private Logger logger = LoggerFactory.getLogger(this.getClass());
  
  /**
   * Create an new driver, with an empty base model
   * 
   * @param inputPathTrain Path of training input file (libsvm format)
   * @param inputPathTest Path of evaluation input file (libsvm format)
   * @param isMultilabelInput True, if the input files are multi-lcass files, false otherwise
   * @param positiveClass ID of the class used as positive class in a one-versus-all classifier (only relevant for multi-class)
   * @param outputPath Output path of the whole job
   * @param highestFeatureId Highest feature id. Typically equal to the number of features
   * @param newtonTolerance Tolerance for Newton-Raphson, e.g. 0.000001. Convergene is assumed if the change in trained coefficient is smaller
   * @param newtonMaxIterations Maximum number of Newton-Raphson iterations, e.g. 5
   * @param regularization L2-regularization penalty term. Set to 0 for no regularization and increase for higher regularization. A high value keeps the coefficient smaller.
   * @param runLocal False, to execute on a cluster, true to execute the job in local mode (via LocalExecutor)
   * @param confPath Path to the config directory of Stratosphere
   * @param jarPath Local path of the job jar file
   */
  public SFODriverPact(
      String inputPathTrain,
      String inputPathTest,
      boolean isMultilabelInput,
      int positiveClass,
      String outputPath,
      int highestFeatureId,
      double newtonTolerance,
      int newtonMaxIterations,
      double regularization,
      boolean runLocal,
      String confPath,
      String jarPath) {
    this.inputPathTrain = inputPathTrain;
    this.inputPathTest = inputPathTest;
    this.isMultilabelInput = isMultilabelInput;
    this.positiveClass = positiveClass;
    this.outputPath = outputPath;
    this.highestFeatureId = highestFeatureId;
    this.newtonTolerance = newtonTolerance;
    this.newtonMaxIterations = newtonMaxIterations;
    this.regularization = regularization;
    this.runLocal = runLocal;
    this.confPath = confPath;
    this.jarPath = jarPath;

    // Create empty model
    this.baseModel = new IncrementalModel(highestFeatureId);
  }
  

  @Override
  public List<FeatureGain> computeGains(int numSubTasks) throws Exception {
    return forwardFeatureSelection(numSubTasks, 1, 1);
  }

  @Override
  public List<FeatureGain> forwardFeatureSelection(int numSubTasks, int iterations, int addPerIteration) throws Exception {

    final Stopwatch stopReadResults = new Stopwatch();

    // RUN
    JobRunner runner = new JobRunner();
    String[] jobArgs = SFOPlanAssembler.buildArgs(
        numSubTasks, 
        inputPathTrain,
        inputPathTest,
        isMultilabelInput,
        positiveClass,
        outputPath,
        highestFeatureId,
        newtonTolerance,
        newtonMaxIterations,
        regularization,
        iterations,
        addPerIteration,
        this.baseModel
        );
    logger.info("Job Args: " + Joiner.on(' ').join(jobArgs));
    
    if (runLocal) {
      Plan sfoPlan = new SFOPlanAssembler().getPlan(jobArgs);
      runner.runLocal(sfoPlan);
    } else {
      runner.run(jarPath, SFOPlanAssembler.class.getName(), jobArgs, confPath, "", "", true);
    }
    counters.put(COUNTER_KEY_TOTAL_WALLCLOCK, runner.getLastWallClockRuntime());
    
    if (iterations <= 1) {
      // Read results from hdfs into memory
      stopReadResults.start();
      this.gains = SFOToolsPact.readGainsAndCoefficients(outputPath);
      stopReadResults.stop();
      counters.put(COUNTER_KEY_READ_RESULT, stopReadResults.elapsed(TimeUnit.MILLISECONDS));
      Collections.sort(this.gains, Collections.reverseOrder());
      return getGains();
    }
    return null;
  }

  @Override
  public void addBestFeatures(int n) throws IOException {
    // Add best to base model
    for (int i=0; i<n; ++i) {
      int bestDimension = gains.get(i).getDimension();
      baseModel.addDimensionToModel(bestDimension,
          gains.get(i).getCoefficient());
      System.out.println("Added d=" + bestDimension
          + " to base model with c="
          + gains.get(i).getCoefficient());
    }

    System.out.println("- New base model: " + baseModel.getW().toString());
  }

  @Override
  public void retrainBaseModel() {
    System.out.println("Retraining base model not yet implemented");
  }

  @Override
  public List<FeatureGain> getGains() {
    return gains;
  }

  @Override
  public long getLastWallClockTime() {
    return counters.get(COUNTER_KEY_TOTAL_WALLCLOCK);
  }

  @Override
  public Map<String, Long> getAllCounters() {
    return counters;
  }

  @Override
  public void resetModel() {
    this.baseModel = new IncrementalModel(highestFeatureId);
  }

}
