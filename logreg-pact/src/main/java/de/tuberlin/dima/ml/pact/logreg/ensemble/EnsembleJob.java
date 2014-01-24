package de.tuberlin.dima.ml.pact.logreg.ensemble;

import de.tuberlin.dima.ml.pact.logreg.eval.CrossEval;
import de.tuberlin.dima.ml.pact.logreg.eval.ReduceEvalSum;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

/**
 * Implements Ensemble Training for Stratosphere-ozone
 * 
 * Uses Mahout SGD to train the model for the individual partitions
 * 
 * Train -> Map -> Reduce -> [(1, i, wi)] -> Reduce(combine) -> (1, 3, w1, w2, w3)
 *  
 *        -> Cross-Eval -> (model-id, partial-total, partial-correct)
 *  Test  /
 *  
 *  -> Reduce-sum -> (model-id, total, correct)
 * 
 * How to run on a cluster:
 * .../bin/pact-client.sh -w -j job.jar -a 1 [job args]
 * 
 * @author Andr�� Hacker
 */
public class EnsembleJob implements Program, ProgramDescription {
  
  @Override
  public String getDescription() {
    return "Parameters: [numPartitions] [inputPathTrain] [inputPathTest] [outputPath] [numFeatures] [runValidation (0 or 1)]";
  }
  
  @Override
  public Plan getPlan(String... args) {
    
    // parse job parameters
    if (args.length < 6) return null;
    int numPartitions  = Integer.parseInt(args[0]);
    String inputPathTrain = args[1];
    String inputPathTest = args[2];
    String outputPath = args[3];
    int numFeatures =  Integer.parseInt(args[4]);
    boolean runValidation = (args[5].equals("1")) ? true : false;

    FileDataSource source = new FileDataSource(TextInputFormat.class, inputPathTrain, "Train Input");
    source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");     // comment out this line for UTF-8 inputs
    
    MapOperator mapRandomPartitioning = MapOperator.builder(MapRandomPartitioning.class)
        .input(source)
        .name("Map: Random Partitioning")
        .build();
    mapRandomPartitioning.getParameters().setInteger(MapRandomPartitioning.CONF_KEY_NUM_PARTITIONS, numPartitions);
    mapRandomPartitioning.getParameters().setInteger(MapRandomPartitioning.CONF_KEY_NUM_FEATURES, numFeatures);
    
    ReduceOperator reduceTrain = ReduceOperator.builder(ReduceTrainPartition.class, IntValue.class, 0)
        .input(mapRandomPartitioning)
        .name("Reduce: Train Partitions")
        .build();
    reduceTrain.getParameters().setInteger(ReduceTrainPartition.CONF_KEY_NUM_FEATURES, numFeatures);
    
    ReduceOperator reduceCombineModel = ReduceOperator.builder(ReduceFlattenModel.class, IntValue.class, 0)
        .input(reduceTrain)
        .name("Reduce: Combine models to a single model")
        .build();
    
    FileDataSink out = null;
    
    if (runValidation) {

      FileDataSource sourceTest = new FileDataSource(TextInputFormat.class, inputPathTest, "Test Input");
      sourceTest.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");

      CrossOperator crossEval = CrossOperator.builder(CrossEval.class)
          .input1(reduceCombineModel)
          .input2(sourceTest)
          .name("Cross: Evaluation")
          .build();
      crossEval.getParameters().setInteger(CrossEval.CONF_KEY_NUM_FEATURES, numFeatures);
      
      ReduceOperator reduceEvalSum = ReduceOperator.builder(ReduceEvalSum.class, StringValue.class, ReduceEvalSum.IDX_MODEL_ID)
      .input(crossEval)
      .name("Reduce: Eval Sum Up")
      .build();

      out = new FileDataSink(CsvOutputFormat.class, outputPath, reduceEvalSum, "Ensemble Validation");
      CsvOutputFormat.configureRecordFormat(out)
      .recordDelimiter('\n')
      .fieldDelimiter(' ')
      .field(IntValue.class, 0)
      .field(IntValue.class, 1)
      .field(IntValue.class, 2);
    } else {    // if (runValidation)
      
      out = new FileDataSink(CsvOutputFormat.class, outputPath, reduceCombineModel, "Ensemble Models");
      CsvOutputFormat.configureRecordFormat(out)
      .recordDelimiter('\n')
      .fieldDelimiter(' ')
      .field(IntValue.class, 0)      // Model id
      .field(IntValue.class, 1)       // Total
      .field(PactVector.class, 2);      // Correct
    }
    
    Plan plan = new Plan(out, "WordCount Example");
    plan.setDefaultParallelism(numPartitions);
    
    return plan;
  }

}
