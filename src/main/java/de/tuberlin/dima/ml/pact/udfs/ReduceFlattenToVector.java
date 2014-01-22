package de.tuberlin.dima.ml.pact.udfs;

import java.util.Iterator;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Input: A set of records with a k distinct key and a numeric value boundled
 * with a dimension.<br/>
 * Output: k records, where each record has a key and a mahout vector containing
 * all numeric values that had this key.<br/>
 * 
 * Why did I not use the PactRecord as a Vector? Because I am not sure if it is
 * as efficient as mahouts vector implementation
 */
public class ReduceFlattenToVector extends ReduceStub {
  
  public static final int IDX_DIMENSION = 0;
  public static final int IDX_DOUBLE_VALUE = 1;
  public static final int IDX_KEY_CONST_ONE = 2;
  
  public static final int IDX_OUT_VECTOR = 0;
  public static final int IDX_OUT_KEY_CONST_ONE = 1;

  public static final String CONF_KEY_NUM_FEATURES = "parameter.NUM_FEATURES";
  private int numFeatures;
  
//  private static final Log logger = LogFactory.getLog(ReduceFlattenToVector.class);
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, 0);
    if (this.numFeatures < 1) {
      throw new IllegalStateException("No (or invalid) value for the mandatory parameter: " + CONF_KEY_NUM_FEATURES);
    }
  }
  
  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {
    Vector vector = new DenseVector(numFeatures);
    PactRecord record = null;
    while (records.hasNext()) {
      record = records.next();
      vector.set(
          record.getField(IDX_DIMENSION, PactInteger.class).getValue(), 
          record.getField(IDX_DOUBLE_VALUE, PactDouble.class).getValue());
    }
    int key = record.getField(IDX_KEY_CONST_ONE, PactInteger.class).getValue();
    PactRecord recordOut = new PactRecord(2);
    recordOut.setField(IDX_OUT_KEY_CONST_ONE, new PactInteger(key));
    recordOut.setField(IDX_OUT_VECTOR, new PactVector(vector));
    out.collect(recordOut);
  }

}
