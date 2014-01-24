package de.tuberlin.dima.ml.pact.logreg.ensemble;

import java.util.Iterator;

import de.tuberlin.dima.ml.pact.logreg.eval.CrossEval;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class ReduceFlattenModel extends ReduceFunction {
  
  public static final int IDX_MODEL_ID = 0;
  public static final int IDX_PARTITION = 1;
  public static final int IDX_MODEL = 2;
  
  Record recordOut = new Record();

  @Override
  public void reduce(Iterator<Record> records, Collector<Record> out)
      throws Exception {
    
    Record record = null;
    int numModels=0;
    while (records.hasNext()) {
      record = records.next();
      recordOut.setField(
          CrossEval.IDX_INPUT1_FIRST_MODEL + numModels,
          record.getField(IDX_MODEL, PactVector.class));
      
      ++numModels;
    }
    recordOut.setField(CrossEval.IDX_INPUT1_NUM_MODELS, new IntValue(numModels));
    
    recordOut.setField(CrossEval.IDX_INPUT1_MODEL_ID, 
        record.getField(IDX_MODEL_ID, IntValue.class));
    
    out.collect(recordOut);
  }

}
