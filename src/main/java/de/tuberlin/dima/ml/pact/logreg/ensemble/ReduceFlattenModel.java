package de.tuberlin.dima.ml.pact.logreg.ensemble;

import java.util.Iterator;

import de.tuberlin.dima.ml.pact.logreg.eval.CrossEval;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class ReduceFlattenModel extends ReduceStub {
  
  public static final int IDX_MODEL_ID = 0;
  public static final int IDX_PARTITION = 1;
  public static final int IDX_MODEL = 2;
  
  PactRecord recordOut = new PactRecord();

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {
    
    PactRecord record = null;
    int numModels=0;
    while (records.hasNext()) {
      record = records.next();
      recordOut.setField(
          CrossEval.IDX_INPUT1_FIRST_MODEL + numModels,
          record.getField(IDX_MODEL, PactVector.class));
      
      ++numModels;
    }
    recordOut.setField(CrossEval.IDX_INPUT1_NUM_MODELS, new PactInteger(numModels));
    
    recordOut.setField(CrossEval.IDX_INPUT1_MODEL_ID, 
        record.getField(IDX_MODEL_ID, PactInteger.class));
    
    out.collect(recordOut);
  }

}
