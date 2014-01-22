package de.tuberlin.dima.ml.pact.logreg.eval;

import java.util.Iterator;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ReduceEvalSum extends ReduceStub{
  
  public static final int IDX_MODEL_ID = 0;
  public static final int IDX_TOTAL = 1;
  public static final int IDX_CORRECT = 2;

  /*
   * This model usually forwards the result to a file sink,
   * so it has to say where the results are written to 
   */
  public static final int IDX_OUT_MODEL_ID = 0;
  public static final int IDX_OUT_TOTAL = 1;
  public static final int IDX_OUT_CORRECT = 2;

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {
    
    int total = 0;
    int correct = 0;
    PactRecord record = null;
    while(records.hasNext()) {
      record = records.next();
      total += record.getField(IDX_TOTAL, PactInteger.class).getValue();
      correct += record.getField(IDX_CORRECT, PactInteger.class).getValue();
    }
    PactInteger modelId = record.getField(IDX_MODEL_ID, PactInteger.class);

    System.out.println("ACCURACY (training-data): " + ((double)correct / (double)total) + " (= " + correct + " / " + total + ")");
    
    // TODO Collect results (model and evaluation)
    PactRecord recordOut = new PactRecord();
    recordOut.setField(IDX_OUT_MODEL_ID, modelId);
    recordOut.setField(IDX_OUT_TOTAL, new PactInteger(total));
    recordOut.setField(IDX_OUT_CORRECT, new PactInteger(correct));
  }

}
