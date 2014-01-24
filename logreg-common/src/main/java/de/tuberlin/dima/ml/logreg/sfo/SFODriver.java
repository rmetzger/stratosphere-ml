package de.tuberlin.dima.ml.logreg.sfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for implementations of Single Feature Optimization Algorithm (SFO)
 * as proposed by Singh et al. [1] using Hadoop.
 * 
 * SFO allows to rank a large number of features regarding their usefulness for
 * classification. Each feature will be evaluated independently by computing how
 * much gain it brings when being added to the current model (base model). The
 * current implementation always starts with an empty base model.<br>
 * 
 * SFO can be used as a forward feature selection method, where in each
 * iteration one or multiple best features are being added to the current base
 * models. SFO is designed to operate on sparse input, e.g. on text.<br>
 * 
 * SFO is a wrapper feature selection method, because it uses a logistic
 * regression training method (Newton-Raphson) internally for the training and
 * ranking. We use log-likelihood maximization for training and for the
 * computation of the gain, but other metrics could be used as well. This means
 * that the ranking only refers to the usefulness for logistic regression.
 * 
 * The efficiency of SFO arises from the fact that the ranking is computed
 * based on approximate models. Please refer to [1] for details.
 * 
 * [1] Singh, S., Kubica, J., Larsen, S., & Sorokina, D. (2009). Parallel Large
 * Scale Feature Selection for Logistic Regression. Optimization, 1172–1183.
 * 
 * @author André Hacker
 */
public interface SFODriver {

  /**
   * Run a single iteration of SFO.
   * Reads the results of the job at the end of the run.
   * 
   * @author André Hacker
   * @param numSubTasks
   *          Total degree of parallelism (dop). This is the total number of
   *          tasks to be executed in parallel, i.e. the number of machines to
   *          be involved times the number on tasks to run on each machine
   *          (inter-node dop * intra-node dop)
   * @return The ranking after the last iteration
   */
  public List<FeatureGain> computeGains(int numSubTasks) throws Exception;

  /**
   * Run SFO as forward feature selection: In each iteration, compute the
   * ranking and add the best features to the base model.
   * 
   * This is equal to computeGains(dop) if iterations = 1
   * 
   * @param numSubTasks
   *          Total degree of parallelism (dop). This is the total number of
   *          tasks to be executed in parallel, i.e. the number of machines to
   *          be involved times the number on tasks to run on each machine
   *          (inter-node dop * intra-node dop)
   * @param iterations
   *          number of iterations.
   * @param addPerIteration
   *          number of best features to be added to the base model in each
   *          iteration
   * @return The ranking after the last iteration
   */
  public List<FeatureGain> forwardFeatureSelection(int numSubTasks, int iterations, int addPerIteration) throws Exception;

  /**
   * Adds the best features from the last run of computeGains() to the base
   * model. Thus it requires computeGains to be executed before.
   * 
   * @param numFeatures number of best features to be added to the base model
   */
  public void addBestFeatures(int numFeatures) throws IOException;
  
  /**
   * Runs a complete training of the model, considering only the coefficients
   * that are in the base model. This is used in a forward feature selection
   * scenario: After one or more features were added to the base model we have
   * to retrain the complete model, because the model trained during the ranking
   * is just an approximate model.
   */
  public void retrainBaseModel();

  /**
   * 
   * @return Ranking of the last run of SFO
   */
  public List<FeatureGain> getGains();
  
  /**
   * @return Elapsed time (wall-clock time) for the run of computeGainSFO
   */
  public long getLastWallClockTime();

  /**
   * @return All available counters (also timers), e.g. the time for training, testing, ...
   */
  public Map<String, Long> getAllCounters();
  
  /**
   * Reset the model to the initial (empty) model
   */
  public void resetModel();
  
}
