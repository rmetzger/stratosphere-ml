package de.tuberlin.dima.ml.pact.logreg.sfo.udfs;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.logreg.LogRegMath;
import de.tuberlin.dima.ml.logreg.sfo.IncrementalModel;
import de.tuberlin.dima.ml.logreg.sfo.SFOGlobalSettings;
import de.tuberlin.dima.ml.pact.logreg.sfo.PactIncrementalModel;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;


/**
 * This UDF processes one input record at a time, where each record consists of
 * a sparse vector x_i (the input) and the label (y_i).
 * 
 * It emits all values that are required for the subsequent training, which will
 * be done independently for each dimension (see below in the code).
 * 
 * We use Cross because we need an instance of the base model in the UDF.
 * 
 * @author André Hacker
 */
public class TrainComputeProbabilities extends CrossFunction {
  
  public static final int IDX_INPUT1_INPUT_RECORD = 0;
  public static final int IDX_INPUT1_LABEL = 1;
  
  public static final int IDX_INPUT2_BASEMODEL = 0;

  public static final int IDX_OUT_DIMENSION = TrainDimensions.IDX_DIMENSION;
  public static final int IDX_OUT_LABEL = TrainDimensions.IDX_LABEL;
  public static final int IDX_OUT_XID = TrainDimensions.IDX_XID;
  public static final int IDX_OUT_PI = TrainDimensions.IDX_PI;
  
  private boolean baseModelCached = false;
  private IncrementalModel baseModel = null;

  private final Record recordOut = new Record();
  
  @Override
  public void open(Configuration parameters) throws Exception {
	// When using iterations, the udf instance will stay the same. We have to deserialize again.
	baseModelCached = false;
  }
  
  // This is inefficient - the system will send us a new copy of the model
  // record for every call, although it is constant.
  @Override
  public void cross(Record trainingVector, Record model,
      Collector<Record> out) throws Exception {

    int yi = trainingVector.getField(IDX_INPUT1_INPUT_RECORD, IntValue.class).getValue();
    Vector xi = trainingVector.getField(IDX_INPUT1_LABEL, PactVector.class).getValue();

    // Optimization: Cache basemodel, will always be the same
    if (!baseModelCached) {
      baseModel = model.getField(IDX_INPUT2_BASEMODEL, PactIncrementalModel.class).getValue();
      baseModelCached = true;
    }
    
    // Compute the prediction for the current input vector using the base model
    double pi = LogRegMath.predict(xi, baseModel.getW(), SFOGlobalSettings.INTERCEPT);
    
	// This loop iterates over all non-zero features in the current input record
	// If there is a value for dimension d, we transmit this value together
	// with the label and the prediction for the base model
	// Only these values are needed for the subsequent training of the feature.
    for (Vector.Element feature : xi.nonZeroes()) {
      if (! baseModel.isFeatureUsed(feature.index())) {
        recordOut.setField(IDX_OUT_DIMENSION, new IntValue(feature.index()));
        recordOut.setField(IDX_OUT_LABEL, new IntValue(yi));
        recordOut.setField(IDX_OUT_XID, new DoubleValue(feature.get()));
        recordOut.setField(IDX_OUT_PI, new DoubleValue(pi));
        out.collect(recordOut);
      }
    }
  }

}
