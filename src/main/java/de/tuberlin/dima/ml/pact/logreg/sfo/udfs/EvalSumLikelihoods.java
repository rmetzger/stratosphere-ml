package de.tuberlin.dima.ml.pact.logreg.sfo.udfs;

import java.util.Iterator;

import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 * For every call, this UDF sums up the gain for a single feature.
 * 
 * @author André Hacker
 */
@Combinable
@ConstantFields({0})
public class EvalSumLikelihoods extends ReduceStub {
  
  public static final int IDX_DIMENSION = 0;
  public static final int IDX_LL_BASE = 1;
  public static final int IDX_LL_NEW = 2;
  
  // Attention: IDX_INPUT1_DIMENSION has to be 0, because we defined this field to be constant 
  public static final int IDX_OUT_DIMENSION = IDX_DIMENSION;
  public static final int IDX_OUT_GAIN = ApplyBest.IDX_INPUT1_GAIN;
  // This field is required because the keyless reducer did not work when using iterations
  public static final int IDX_OUT_KEY_CONST_ONE = 2;

  PactRecord recordOut = new PactRecord(2);
  
  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {
    
    PactRecord record = null;
    double gain = 0;
    while (records.hasNext()) {
      record = records.next();
      gain += record.getField(IDX_LL_BASE, PactDouble.class).getValue();
    }
    
    recordOut.copyFrom(record, new int[] {IDX_DIMENSION}, new int[] {IDX_OUT_DIMENSION});
    recordOut.setField(IDX_OUT_KEY_CONST_ONE, PactUtils.pactOne);
    recordOut.setField(IDX_OUT_GAIN, new PactDouble(gain));
    out.collect(recordOut);
  }

  PactRecord recordOutCombine = new PactRecord(2);
  
  @Override
  public void combine(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {

    PactRecord record = null;
    double gain = 0;
    while (records.hasNext()) {
      record = records.next();
      gain += record.getField(IDX_LL_BASE, PactDouble.class).getValue();
    }
    
    recordOutCombine.copyFrom(record, new int[] {IDX_DIMENSION}, new int[] {IDX_DIMENSION});
    recordOutCombine.setField(IDX_LL_BASE, new PactDouble(gain));
    out.collect(recordOutCombine);
  }

}
