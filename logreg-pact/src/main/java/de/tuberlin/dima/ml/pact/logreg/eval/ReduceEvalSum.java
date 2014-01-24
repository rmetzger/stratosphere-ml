package de.tuberlin.dima.ml.pact.logreg.eval;

import java.util.Iterator;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class ReduceEvalSum extends ReduceFunction {
  
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
  public void reduce(Iterator<Record> records, Collector<Record> out)
      throws Exception {
    
    int total = 0;
    int correct = 0;
    Record record = null;
    while(records.hasNext()) {
      record = records.next();
      total += record.getField(IDX_TOTAL, IntValue.class).getValue();
      correct += record.getField(IDX_CORRECT, IntValue.class).getValue();
    }
    IntValue modelId = record.getField(IDX_MODEL_ID, IntValue.class);

    System.out.println("ACCURACY (training-data): " + ((double)correct / (double)total) + " (= " + correct + " / " + total + ")");
    
    // TODO Collect results (model and evaluation)
    Record recordOut = new Record();
    recordOut.setField(IDX_OUT_MODEL_ID, modelId);
    recordOut.setField(IDX_OUT_TOTAL, new IntValue(total));
    recordOut.setField(IDX_OUT_CORRECT, new IntValue(correct));
    out.collect(recordOut);
  }

}
