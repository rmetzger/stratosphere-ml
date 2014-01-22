package de.tuberlin.dima.ml.pact.logreg.sfo.udfs;

import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * This UDF is used to join the trained coefficients and the gain for each
 * dimensions, since both are the output of different UDFs.
 * 
 * @author André Hacker
 */
@ConstantFields({0, 1})
public class MatchGainsAndCoefficients extends MatchStub {
  
  public static final int IDX_INPUT1_DIMENSION = 0;
  public static final int IDX_INPUT1_GAIN = 1;
  
  public static final int IDX_INPUT2_DIMENSION = 0;
  public static final int IDX_INPUT2_COEFFICIENT = 1;
  
  public static final int IDX_OUT_DIMENSION = IDX_INPUT1_DIMENSION;
  public static final int IDX_OUT_GAIN = IDX_INPUT1_GAIN;
  public static final int IDX_OUT_COEFFICIENT = 2;
  public static final int IDX_OUT_KEY_CONST_ONE = 3;

//  private static final Log logger = LogFactory.getLog(MatchGainsAndCoefficients.class);
  
  @Override
  public void match(PactRecord gain, PactRecord coefficient,
      Collector<PactRecord> out) throws Exception {

    gain.copyFrom(coefficient, new int[] {IDX_INPUT2_COEFFICIENT}, 
        new int[] {IDX_OUT_COEFFICIENT});
    gain.setField(IDX_OUT_KEY_CONST_ONE, PactUtils.pactOne);
    
    out.collect(gain);
  }


}
