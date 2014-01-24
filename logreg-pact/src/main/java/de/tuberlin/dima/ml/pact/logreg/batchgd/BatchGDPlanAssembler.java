/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package de.tuberlin.dima.ml.pact.logreg.batchgd;

import de.tuberlin.dima.ml.pact.JobRunner;
import de.tuberlin.dima.ml.pact.io.LibsvmInputFormat;
import de.tuberlin.dima.ml.pact.logreg.ensemble.EnsembleJob;
import de.tuberlin.dima.ml.pact.types.PactVector;
import de.tuberlin.dima.ml.util.IOUtils;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.api.common.operators.BulkIteration;

public class BatchGDPlanAssembler implements ProgramDescription {

//  private static final int ECAT = 59;
//  private static final int GCAT = 70;
//  private static final int MCAT = 102;

  private static final int INITIAL_VALUE = 0;

  @Override
  public Plan getPlan(String... args) {
    // parse job parameters
    final int numSubTasks = (args.length > 0) ? Integer.parseInt(args[0]) : 1;
    final String inputPathTrain = (args.length > 1) ? args[1] : "";
//    final String inputPathTest = (args.length > 2) ? args[2] : "";
    final String outputPath = (args.length > 3) ? args[3] : "";
    final int numIterations = (args.length > 4) ? Integer.parseInt(args[4]) : 1;
//    final boolean runValidation = (args.length > 5) ? ((args[5].equals("1")) ? true
//        : false)
//        : false;
    final String learningRate = (args.length > 6) ? args[6] : "1";
    final int ccat = (args.length > 7) ? Integer.parseInt(args[7]) : 1;
    final int features = (args.length > 8) ? Integer.parseInt(args[8]) : 47237;

    // input vectors (constant path)
    FileDataSource trainingVectors = new FileDataSource(
        LibsvmInputFormat.class, inputPathTrain, "Input Vectors");
    trainingVectors.setParameter(LibsvmInputFormat.CONF_KEY_POSITIVE_CLASS, ccat);
    trainingVectors.setParameter(LibsvmInputFormat.CONF_KEY_NUM_FEATURES,
        features);

    // initial weight
    GenericDataSource<WeightVectorInputFormat> initialWeights = new GenericDataSource<WeightVectorInputFormat>(WeightVectorInputFormat.class);
    initialWeights.setParameter(WeightVectorInputFormat.CONF_KEY_NUM_FEATURES, features);
    initialWeights.setParameter(WeightVectorInputFormat.CONF_KEY_INITIAL_VALUE, INITIAL_VALUE);

    BulkIteration iteration = new BulkIteration("Batch GD");
    iteration.setInput(initialWeights);
    iteration.setMaximumNumberOfIterations(numIterations);
    System.out.println("NUM ITERATIONS: " + numIterations);

    CrossOperator computeGradientParts = CrossOperator.builder(ComputeGradientParts.class)
        .input1(trainingVectors)
        .input2(iteration.getPartialSolution())
        .name("Compute Gradient Parts (Cross)")
        .build();

    // TODO Stratosphere Bug: If we don't pass the key class and the key column,
    // we get the unspecific error
    // Exception in thread "main"
    // eu.stratosphere.pact.compiler.CompilerException: Unknown local strategy:
    // ALL_GROUP at
    // eu.stratosphere.pact.compiler.costs.CostEstimator.costOperator(CostEstimator.java:185)
    ReduceOperator computeGradient = ReduceOperator.builder(GradientSumUp.class, IntValue.class, 0)
        .input(computeGradientParts)
        .name("Sum up Gradient (Reduce)")
        .build();
    
    CrossOperator applyGradient = CrossOperator.builder(ApplyGradient.class)
        .input1(iteration.getPartialSolution())
        .input2(computeGradient)
        .name("Apply Gradient (Cross)")
        .build();
    applyGradient.setParameter(ApplyGradient.CONF_KEY_LEARNING_RATE, learningRate);

    iteration.setNextPartialSolution(applyGradient);

    FileDataSink finalResult = new FileDataSink(CsvOutputFormat.class,
        outputPath, iteration, "Output");

    CsvOutputFormat.configureRecordFormat(finalResult).recordDelimiter('\n')
        .fieldDelimiter(' ')
        .field(PactVector.class, 0);

    Plan plan = new Plan(finalResult, "BatchGD Plan");
    plan.setDefaultParallelism(numSubTasks);

    return plan;
  }

  @Override
  public String getDescription() {
    return "[numSubTasks] [inputPathTrain] [inputPathTest] [outputPath] [numIteration] [runValidation (0 or 1)] [learningRate] [positiveClass] [numFeatures]";
  }

  public static void main(String[] args) throws Exception {
    BatchGDPlanAssembler bgd = new BatchGDPlanAssembler();

    String numSubTasks = "1";
    String inputFileTrain = "file:///Users/qml_moon/Documents/TUB/DIMA/code/lr";
    String inputFileTest = "file:///Users/qml_moon/Documents/TUB/DIMA/code/lr";
    String outputFile = "file:///Users/qml_moon/Documents/TUB/DIMA/code/lr-out";
    String numIterations = "5";
    String runValidation = "0";
    String learningRate = "0.05";
    String[] jobArgs = { 
        numSubTasks, 
        inputFileTrain, 
        inputFileTest,
        outputFile,
        numIterations,
        runValidation,
        learningRate};
    // String[] jobArgs = { "1", "file:///home/andre/dev/logreg-repo",
    // "file:///Users/uce/Desktop/rcv1libsvm/rcv1_topics_train.svm",
    // "file:///Users/uce/Desktop/rcv1libsvm/rcv1_topics_train.svm",
    // "file:///Users/uce/Desktop/rcv1libsvm/output/", "1", "0" };
    
    boolean runLocal = true;
    JobRunner runner = new JobRunner();
    if (runLocal) {
      
      runner.runLocal(bgd.getPlan(jobArgs));
      
    } else {
      
      String jarPath = IOUtils.getDirectoryOfJarOrClass(EnsembleJob.class)
          + "/logreg-pact-0.0.1-SNAPSHOT-job.jar";
      System.out.println("JAR PATH: " + jarPath);
      runner.run(jarPath, "de.tuberlin.dima.ml.pact.logreg.batchgd.BatchGDJob", jobArgs, "", "", "", true);
      
    }
    System.out.println("Job completed. Runtime=" + runner.getLastWallClockRuntime());

  }

}