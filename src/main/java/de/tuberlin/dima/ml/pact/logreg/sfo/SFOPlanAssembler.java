package de.tuberlin.dima.ml.pact.logreg.sfo;

import java.io.IOException;

import com.google.common.base.Joiner;

import de.tuberlin.dima.ml.logreg.sfo.IncrementalModel;
import de.tuberlin.dima.ml.logreg.sfo.SFODriver;
import de.tuberlin.dima.ml.pact.io.LibsvmInputFormat;
import de.tuberlin.dima.ml.pact.io.SingleValueDataSource;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.ApplyBest;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.EvalComputeLikelihoods;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.EvalSumLikelihoods;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.MatchGainsAndCoefficients;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.TrainComputeProbabilities;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.TrainDimensions;
import de.tuberlin.dima.ml.pact.udfs.CrossTwoToOne;
import de.tuberlin.dima.ml.pact.udfs.ReduceFlattenToVector;
import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.generic.contract.BulkIteration;
import eu.stratosphere.pact.generic.contract.Contract;

/**
 * Plan assembler for SFO (see {@link SFODriver}).
 * 
 * Depending on the parameters this plan will either use bulk iterations or not.
 * If iterations=1, it will execute a single iteration
 * 
 * @author Andre Hacker
 */
public class SFOPlanAssembler implements PlanAssembler, PlanAssemblerDescription {

  @Override
  public String getDescription() {
    return "Parameters: <numSubStasks> <inputPathTrain> <inputPathTest> <isMultiLabel (true/false)> <positiveClass>"
        + " <outputPath> <numFeatures> <newton tolerance> <newton max iterations> <regularization> <iterations> <addPerIteration>" 
        + " <Optional: baseModel (base64 encoded)>";
  }
  
  /**
   * This method is a convenience method. It is very error prone to use the
   * string-args so I added this constructor that can be used whenever this plan
   * is executed from within code.
   * 
   * @param numSubTasks
   * @param inputPathTrain Path of training input file (libsvm format)
   * @param inputPathTest Path of evaluation input file (libsvm format)
   * @param isMultilabelInput True, if the input files are multi-lcass files, false otherwise
   * @param positiveClass ID of the class used as positive class in a one-versus-all classifier (only relevant for multi-class)
   * @param outputPath Output path of the whole job
   * @param numFeatures Highest feature id. Typically equal to the number of features
   * @param newtonTolerance Tolerance for Newton-Raphson, e.g. 0.000001. Convergene is assumed if the change in trained coefficient is smaller
   * @param newtonMaxIterations Maximum number of Newton-Raphson iterations, e.g. 5
   * @param regularization L2-regularization penalty term. Set to 0 for no regularization and increase for higher regularization. A high value keeps the coefficient smaller.
   * @param iterations number of iterations. The bulk iterations feature will only be used if iterations is greater than 1
   * @param addPerIteration The number of features to be added to the base model. Only considered if iterations>1.
   * @param baseModel The instance of the current base model
   * @return the string array that can be used to start the job
   */
  public static String[] buildArgs(
      int numSubTasks, 
      String inputPathTrain, 
      String inputPathTest,
      boolean isMultilabelInput,
      int positiveClass,
      String outputPath,
      int numFeatures,
      double newtonTolerance,
      int newtonMaxIterations,
      double regularization,
      int iterations,
      int addPerIteration,
      IncrementalModel baseModel) {
    return new String[] {
        Integer.toString(numSubTasks),
        inputPathTrain,
        inputPathTest,
        Boolean.toString(isMultilabelInput),
        Integer.toString(positiveClass),
        outputPath,
        Integer.toString(numFeatures),
        Double.toString(newtonTolerance),
        Integer.toString(newtonMaxIterations),
        Double.toString(regularization),
        Integer.toString(iterations),
        Integer.toString(addPerIteration),
        PactUtils.encodeValueAsBase64(new PactIncrementalModel(baseModel))
    };
  }

  @Override
  public Plan getPlan(String... args) {
    System.out.println("getPlan(" + Joiner.on(' ').join(args) + ")");
    
    // The default values just exist to be able to view this job in pact-web.
    
    int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
    String inputPathTrain = (args.length > 1 ? args[1] : "");
    String inputPathTest = (args.length > 2 ? args[2] : "");
    boolean isMultilabelInput = (args.length > 3 ? Boolean.parseBoolean(args[3]) : true);
    int positiveClass = (args.length > 4 ? Integer.parseInt(args[4]) : 0);
    String outputPath = (args.length > 5 ? args[5] : "");
    int numFeatures = (args.length > 6 ? Integer.parseInt(args[6]) : 0);
    double newtonTolerance = (args.length > 7 ? Double.parseDouble(args[7]) : 0);
    int newtonMaxIterations = (args.length > 8 ? Integer.parseInt(args[8]) : 0);
    double regularization = (args.length > 9 ? Double.parseDouble(args[9]) : 0);
    int iterations = (args.length > 10 ? Integer.parseInt(args[10]) : 1);
    int addPerIteration = (args.length > 11 ? Integer.parseInt(args[11]) : 1);
    IncrementalModel baseModel = (args.length > 12 ? PactUtils.decodeValueFromBase64(args[12], PactIncrementalModel.class).getValue() : new IncrementalModel(numFeatures));
    
    // ----- HINTS / OPTIMIZATION -----
    
    boolean giveBroadcastHints = true;
    boolean giveCardinalityHints = true;
    boolean giveFineGradeDopHints = true;
    if (iterations > 1) {
      // dop has to be the same for all contracts when using iterations. Otherwise you receive this error:
      // Error: All functions that are part of an iteration must have the same degree-of-parallelism as that iteration.
      giveFineGradeDopHints = false;
    }
    
    // ----- Data Sources -----
    
    FileDataSource trainingVectors = new FileDataSource(
        LibsvmInputFormat.class, inputPathTrain, "Training Input");
    trainingVectors.setParameter(LibsvmInputFormat.CONF_KEY_NUM_FEATURES,
        numFeatures);
    trainingVectors.setParameter(LibsvmInputFormat.CONF_KEY_MULTI_LABEL_INPUT, isMultilabelInput);
    if (isMultilabelInput) {
      trainingVectors.setParameter(LibsvmInputFormat.CONF_KEY_POSITIVE_CLASS, positiveClass);
    }
    
    FileDataSource testVectors = new FileDataSource(
        LibsvmInputFormat.class, inputPathTest, "Test Input");
    testVectors.setParameter(LibsvmInputFormat.CONF_KEY_NUM_FEATURES,
        numFeatures);
    testVectors.setParameter(LibsvmInputFormat.CONF_KEY_MULTI_LABEL_INPUT, isMultilabelInput);
    if (isMultilabelInput) {
      testVectors.setParameter(LibsvmInputFormat.CONF_KEY_POSITIVE_CLASS, positiveClass);
    }

    // ----- Initial Base Model -----
    
    Contract initialBaseModelContract = null;
    if (baseModel != null && baseModel.getUsedDimensions().size() > 0) {
      try {
//        String baseModelTmpPath = "file:///tmp/tmp-base-model";
    	String baseModelTmpPath = "hdfs://cloud-11:45010/tmp-base-model";
        initialBaseModelContract = new SingleValueDataSource(new PactIncrementalModel(baseModel), baseModelTmpPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      initialBaseModelContract = new GenericDataSource<EmptyBaseModelInputFormat>(EmptyBaseModelInputFormat.class);
      initialBaseModelContract.setParameter(EmptyBaseModelInputFormat.CONF_KEY_NUM_FEATURES, numFeatures);
    }
    initialBaseModelContract.setName("BaseModel");
    if (giveFineGradeDopHints) {
      initialBaseModelContract.setDegreeOfParallelism(1);
    }

    // ----- Iterations -----
    
    Contract baseModelContract = null;
    BulkIteration iteration = null;
    if (iterations > 1) {
      iteration = new BulkIteration("Forward Feature Selection");
//    iteration.setDegreeOfParallelism(numSubTasks);
      iteration.setInput(initialBaseModelContract);
      iteration.setMaximumNumberOfIterations(iterations);
      baseModelContract = iteration.getPartialSolution();
    } else {
      baseModelContract = initialBaseModelContract;
    }
    
    // ----- Cross: Train over x -----
    
    CrossContract trainComputeProbabilities = CrossContract.builder(TrainComputeProbabilities.class)
        .input1(trainingVectors)
        .input2(baseModelContract)
        .name("Train: Compute probabilities (Cross)")
        .build();
    if (giveBroadcastHints) {
      trainComputeProbabilities.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
          PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
      trainComputeProbabilities.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
          PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
    }
    
    // ----- Reduce: Train over d -----
    
    ReduceContract trainDimensions = ReduceContract.builder(TrainDimensions.class, PactInteger.class, TrainDimensions.IDX_DIMENSION)
        .input(trainComputeProbabilities)
        .name("Train: Train new Features (Reduce)")
        .build();
    System.out.println("Newton tolerance: " + Double.toString(newtonTolerance));
    trainDimensions.setParameter(TrainDimensions.CONF_KEY_NEWTON_MAX_ITERATIONS, newtonMaxIterations);
    trainDimensions.setParameter(TrainDimensions.CONF_KEY_NEWTON_TOLERANCE, Double.toString(newtonTolerance));
    trainDimensions.setParameter(TrainDimensions.CONF_KEY_REGULARIZATION, Double.toString(regularization));
    
    // ----- Workaround 1: Flatten Coefficients -----
    
//    ReduceContract flattenCoefficients = ReduceContract.builder(ReduceFlattenToVector.class)
    ReduceContract flattenCoefficients = ReduceContract.builder(ReduceFlattenToVector.class, PactInteger.class, ReduceFlattenToVector.IDX_KEY_CONST_ONE)
        .input(trainDimensions)
        .name("Workaround: Flatten trained coefficients (Reduce)")
        .build();
    flattenCoefficients.setParameter(ReduceFlattenToVector.CONF_KEY_NUM_FEATURES, numFeatures);
    
    // ----- Workaround 2: Make 1 out of 2 records -----

    CrossContract basemodelAndCoefficients = CrossContract.builder(CrossTwoToOne.class)
        .input1(baseModelContract)
        .input2(flattenCoefficients)
        .name("Workaround: Flatten two to one (Cross)")
        .build();
    basemodelAndCoefficients.setParameter(CrossTwoToOne.CONF_KEY_IDX_OUT_VALUE1, EmptyBaseModelInputFormat.IDX_OUT_BASEMODEL);
    basemodelAndCoefficients.setParameter(CrossTwoToOne.CONF_KEY_IDX_OUT_VALUE2, ReduceFlattenToVector.IDX_OUT_VECTOR);
    if (giveFineGradeDopHints) {
      basemodelAndCoefficients.setDegreeOfParallelism(1);
      // Didn't find a way to specify that this contract emits just a single record
    }
    
    // ----- Cross: Eval Compute Likelihoods over records -----

    CrossContract evalComputeLikelihoods = CrossContract.builder(EvalComputeLikelihoods.class)
        .input1(testVectors)
        .input2(basemodelAndCoefficients)
        .name("Eval: Compute likelihoods (Cross)")
        .build();
    if (giveBroadcastHints) {
      evalComputeLikelihoods.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT,
          PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
      evalComputeLikelihoods.getParameters().setString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT,
          PactCompiler.HINT_SHIP_STRATEGY_BROADCAST);
    }
    
    // ----- Reduce: Sum up likelihoods -----

    ReduceContract evalSumUpLikelihoods = ReduceContract
        .builder(EvalSumLikelihoods.class, PactInteger.class, EvalSumLikelihoods.IDX_DIMENSION)
        .input(evalComputeLikelihoods)
        .name("Eval: Sum up likelihoods (Reduce)")
        .build();
    
    // ----- Match Gains & Coefficients -----

    MatchContract matchGainsCoefficients = MatchContract
        .builder(MatchGainsAndCoefficients.class, PactInteger.class,
            EvalSumLikelihoods.IDX_OUT_DIMENSION,
            TrainDimensions.IDX_OUT_DIMENSION)
            .input1(evalSumUpLikelihoods)
            .input2(trainDimensions)
            .name("Match Gains and Coefficients")
            .build();
    if (giveCardinalityHints) {
      matchGainsCoefficients.getCompilerHints().setAvgRecordsEmittedPerStubCall(1);
//      matchGainsCoefficients.getCompilerHints().setAvgBytesPerRecord(??);
    }
    
    FileDataSink dataSink = null;
    if (iterations > 1) {
    
      // ----- CoGroup: Sort & Apply best to base model -----
      
      CoGroupContract applyBest = CoGroupContract
          .builder(ApplyBest.class, PactInteger.class,
              MatchGainsAndCoefficients.IDX_OUT_KEY_CONST_ONE,
              EmptyBaseModelInputFormat.IDX_OUT_KEY_CONST_ONE)
          .input1(matchGainsCoefficients)
          .input2(baseModelContract)
          .name("ApplyBest")
          .build();
      applyBest.setParameter(ApplyBest.CONF_KEY_ADD_PER_ITERATION, addPerIteration);
      if (giveFineGradeDopHints) {
        applyBest.setDegreeOfParallelism(1);
      }
      // TODO Sorting of CoGroup did not work!
  //    sortAndApplyBest.setGroupOrderForInputOne(new Ordering(EvalSumLikelihoods.IDX_OUT_GAIN,
  //                PactDouble.class, Order.ASCENDING));
      
      iteration.setNextPartialSolution(applyBest);
      
      // ----- Data Sink & Output Format -----
      
      dataSink = new FileDataSink(RecordOutputFormat.class,
          outputPath, iteration, "Output");

      RecordOutputFormat.configureRecordFormat(dataSink).recordDelimiter('\n')
          .fieldDelimiter(' ')
          .field(PactIncrementalModel.class, ApplyBest.IDX_OUT_BASEMODEL);

    } else {
      
      // ----- Data Sink & Output Format -----
      
      dataSink = new FileDataSink(RecordOutputFormat.class,
          outputPath, matchGainsCoefficients, "Output");

      RecordOutputFormat.configureRecordFormat(dataSink).recordDelimiter('\n')
          .fieldDelimiter(' ')
          .field(PactInteger.class, MatchGainsAndCoefficients.IDX_OUT_DIMENSION)
          .field(PactDouble.class, MatchGainsAndCoefficients.IDX_OUT_GAIN)
          .field(PactDouble.class, MatchGainsAndCoefficients.IDX_OUT_COEFFICIENT);
    }

    // ----- Plan -----

    Plan plan = new Plan(dataSink, "BatchGD Plan");
    plan.setDefaultParallelism(numSubTasks);
    
    return plan;
  }

}
