package de.tuberlin.dima.ml.pact.logreg.batchgd;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.pact.io.RecordSequenceInputFormat;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;

public class WeightVectorInputFormat extends RecordSequenceInputFormat {

  public static final String CONF_KEY_NUM_FEATURES = "weight_vector_input.num_features";
  private int numFeatures = 0;
  
  public static final String CONF_KEY_INITIAL_VALUE = "weight_vector_input.initial_value";
  private int initialValue = 0;
  
  @Override
  public void configure(Configuration parameters) {
    this.numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, -1);
    if (this.numFeatures == -1) {
      throw new IllegalArgumentException("Please specify the value for CONF_KEY_NUM_FEATURES");
    }
    this.initialValue = parameters.getInteger(CONF_KEY_INITIAL_VALUE, Integer.MIN_VALUE);
    if (this.initialValue == Integer.MIN_VALUE) {
      throw new IllegalArgumentException("Please specify the value for CONF_KEY_INITIAL_VALUE");
    }
  }

  @Override
  public long getNumRecords() { return 1; }

  @Override
  public void fillNextRecord(PactRecord record, int recordNumber) {
    Vector vector = new RandomAccessSparseVector(numFeatures);
    if (this.initialValue != 0) vector.assign(initialValue);
    record.setField(0, new PactVector(vector));
  }

}
