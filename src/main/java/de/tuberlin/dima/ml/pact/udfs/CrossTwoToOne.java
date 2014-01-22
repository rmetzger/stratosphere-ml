package de.tuberlin.dima.ml.pact.udfs;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * Copies a value from first and another value from second record to a new
 * single record. Assumption: Each input consists of just a single record.<br>
 * 
 * Workaround, required to overcome the limitation that Cross does not work for
 * more then 3 inputs (and the lack of Distributed cache)
 * 
 * @author Andre Hacker
 */
public class CrossTwoToOne extends CrossStub {
  
  public static int IDX_INPUT1_VALUE = 0;
  public static int IDX_INPUT2_VALUE = 0;
  
  public static final int IDX_OUT_VALUE1 = 0;
  public static final int IDX_OUT_VALUE2 = 1;
  
  public static final String CONF_KEY_IDX_OUT_VALUE1 = "parameter.index-out-value1";
  public static final String CONF_KEY_IDX_OUT_VALUE2 = "parameter.index-out-value2";
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    int overwriteIndex1 = parameters.getInteger(CONF_KEY_IDX_OUT_VALUE1, -1);
    int overwriteIndex2 = parameters.getInteger(CONF_KEY_IDX_OUT_VALUE2, -1);
    if (overwriteIndex1 != -1) {
      IDX_INPUT1_VALUE = overwriteIndex1;
    }
    if (overwriteIndex2 != -1) {
      IDX_INPUT2_VALUE = overwriteIndex2;
    }
  }

  @Override
  public void cross(PactRecord record1, PactRecord record2,
      Collector<PactRecord> out) throws Exception {
    PactRecord recordOut = new PactRecord(2);
    recordOut.copyFrom(record1, new int[]{IDX_INPUT1_VALUE}, new int[]{IDX_OUT_VALUE1});
    recordOut.copyFrom(record2, new int[]{IDX_INPUT2_VALUE}, new int[]{IDX_OUT_VALUE2});
    out.collect(recordOut);
  }

}
