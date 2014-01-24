package de.tuberlin.dima.ml.pact.logreg.eval;

import java.util.ArrayList;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.RegressionModel;
import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;
import de.tuberlin.dima.ml.logreg.LogRegEnsembleModel;
import de.tuberlin.dima.ml.logreg.LogRegEnsembleModel.VotingSchema;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 * Currently evaluates only a single model.
 * 
 * @author Andre Hacker
 *
 */
public class CrossEval extends CrossFunction {
  
  public static final String CONF_KEY_NUM_FEATURES = "parameter.NUM_FEATURES";
  
  public static final int IDX_INPUT1_MODEL_ID = 0;
  public static final int IDX_INPUT1_NUM_MODELS = 1;
  public static final int IDX_INPUT1_FIRST_MODEL = 2;
  
  public static final int IDX_INPUT2_INPUT_RECORD = 0;
  
  private int numFeatures;
  
  private boolean modelCached = false;
  private RegressionModel model = null; // this will be cached in first local run by the udf
  
  private final Record recordOut = new Record();
  
  private static final IntValue zero = new IntValue(0);
  private static final IntValue one = new IntValue(1);
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, 0);
  }

  @Override
  public void cross(Record modelRecord, Record dataRecord,
      Collector<Record> out) throws Exception {
    
    // Read test item
    StringValue line = dataRecord.getField(IDX_INPUT2_INPUT_RECORD, StringValue.class);
    Vector x = new RandomAccessSparseVector(numFeatures);
    int label = LibSvmVectorReader.readVectorSingleLabel(x, line.getValue());

    if (! modelCached) {
      // Read ensemble model
      // TODO Major: Bad that we build the model for every call - should always stay the same!
      int numModels = modelRecord.getField(IDX_INPUT1_NUM_MODELS, IntValue.class).getValue();
      System.out.println("Num Models: " + numModels);
      ArrayList<Vector> ensembleModels = Lists.newArrayList();
      for (int i=0; i<numModels; ++i) {
        ensembleModels.add(modelRecord.getField(IDX_INPUT1_FIRST_MODEL + i, PactVector.class).getValue());
      }
      model = new LogRegEnsembleModel(ensembleModels, 0.5d, VotingSchema.MAJORITY_VOTE);
      modelCached = true;
    }
    
    // TODO Change to ClassificationModel and classify. For EnsembleModel I cannot really predict the probability
    double prediction = model.predict(x);
    recordOut.setField(ReduceEvalSum.IDX_MODEL_ID, one);
    recordOut.setField(ReduceEvalSum.IDX_TOTAL, one);
    recordOut.setField(ReduceEvalSum.IDX_CORRECT, (prediction == label)?one:zero);
    out.collect(recordOut);
  }
  
}
