package de.tuberlin.dima.ml.pact.logreg.sfo;

import de.tuberlin.dima.ml.logreg.sfo.IncrementalModel;
import de.tuberlin.dima.ml.pact.io.RecordSequenceInputFormat;
import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.PactRecord;

/**
 * This input format always emits a single record holding an empty base model.
 * 
 * One could have used a file input for this, but I consider this way to be
 * closer to my original itention (I don't want to add an dependency that the
 * user has to care that the file exists...)
 * 
 * @author Andre Hacker
 */
public class EmptyBaseModelInputFormat extends RecordSequenceInputFormat {
  
  public static final int IDX_OUT_BASEMODEL = 0;
  public static final int IDX_OUT_KEY_CONST_ONE = 1;

  public static final String CONF_KEY_NUM_FEATURES = "empty_basemodel.num_features";
  private int numFeatures = 0;
  
  @Override
  public void configure(Configuration parameters) {
    this.numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, -1);
    if (this.numFeatures == -1) {
      throw new IllegalArgumentException("Please specify the value for CONF_KEY_NUM_FEATURES");
    }
  }

  @Override
  public long getNumRecords() {
	// Changing this to unknown causes that this input does no longer emit anything
//    return BaseStatistics.NUM_RECORDS_UNKNOWN;
    return 1;
  }

  @Override
  public void fillNextRecord(PactRecord record, int recordNumber) {
    IncrementalModel baseModel = new IncrementalModel(numFeatures);
    record.setField(IDX_OUT_BASEMODEL, new PactIncrementalModel(baseModel));
    record.setField(IDX_OUT_KEY_CONST_ONE, PactUtils.pactOne);
  }

}
