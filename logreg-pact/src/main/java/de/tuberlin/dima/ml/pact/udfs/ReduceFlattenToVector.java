package de.tuberlin.dima.ml.pact.udfs;

import java.util.Iterator;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;


/**
 * Input: A set of records with a k distinct key and a numeric value boundled
 * with a dimension.<br/>
 * Output: k records, where each record has a key and a mahout vector containing
 * all numeric values that had this key.<br/>
 * 
 * Why did I not use the Record as a Vector? Because I am not sure if it is
 * as efficient as mahouts vector implementation
 */
public class ReduceFlattenToVector extends ReduceFunction {
  
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
  public void reduce(Iterator<Record> records, Collector<Record> out)
      throws Exception {
    Vector vector = new DenseVector(numFeatures);
    Record record = null;
    while (records.hasNext()) {
      record = records.next();
      vector.set(
          record.getField(IDX_DIMENSION, IntValue.class).getValue(),
          record.getField(IDX_DOUBLE_VALUE, DoubleValue.class).getValue());
    }
    int key = record.getField(IDX_KEY_CONST_ONE, IntValue.class).getValue();
    Record recordOut = new Record(2);
    recordOut.setField(IDX_OUT_KEY_CONST_ONE, new IntValue(key));
    recordOut.setField(IDX_OUT_VECTOR, new PactVector(vector));
    out.collect(recordOut);
  }

}
