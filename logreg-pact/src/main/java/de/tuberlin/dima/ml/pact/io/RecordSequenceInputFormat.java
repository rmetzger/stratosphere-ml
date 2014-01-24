package de.tuberlin.dima.ml.pact.io;

import java.io.IOException;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.api.common.operators.GenericDataSource;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.types.Record;
import eu.stratosphere.api.common.io.InputFormat;

/**
 * Base class for Input Formats that have only a single split and emit a
 * sequence of records. This is designed as a non-file input format where the
 * records to emit are predefined at compile-time. The process of creating the
 * records can be made dynamic to a certain extend by overriding configure (i.e.
 * by sending parameters to the InputFormat).<br/>
 * 
 * Use {@link GenericDataSource} to get a Contract that can be used as an job
 * input. <br/>
 * 
 * It is currently not supported to send more complex objects (implementing
 * Value) during Job construction. This would, however, be a nice feature,
 * because sometimes we have the objects to use as input available at runtime
 * during job construction and we don't want to use the workaround to write to a
 * file and use this as input.
 */
public abstract class RecordSequenceInputFormat implements InputFormat<Record, GenericInputSplit> {
  
  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
int recordsProcessed = 0;
  
  /**
   * The implementation must return the number of records it wants to emit.
   * fillNextRecord() will be called this many times.
   * 
   * @return The number of records this InputFormat will produce
   */
  public abstract long getNumRecords();
  
  /**
   * This method will be called as often as specified in getNumRecords(). This
   * is the place where the implementation can actually create the records.
   * 
   * @param record  The record that has to be filled
   * @param recordNumber  1 if this is the first record, 2 for second, ...
   */
  public abstract void fillNextRecord(Record record, int recordNumber);
  
  /**
   * Default implementation for configure where we don't get any parameters.
   */
  @Override
  public void configure(Configuration parameters) { }

  @Override
  public final BaseStatistics getStatistics(BaseStatistics cachedStatistics)
      throws IOException {
    // When this method is called, configure was called before
    // I guess this is called once only when the job gets compiled
    
    return new BaseStatistics() {
      @Override
      public long getTotalInputSize() {
//        return BaseStatistics.SIZE_UNKNOWN;
    	return 1;
      }
      @Override
      public long getNumberOfRecords() {
        return getNumRecords();
      }
      @Override
      public float getAverageRecordWidth() {
        return BaseStatistics.AVG_RECORD_BYTES_UNKNOWN;
      }
    };
  }

  @Override
  public final GenericInputSplit[] createInputSplits(int minNumSplits)
      throws IOException {
    // return a single split
    GenericInputSplit[] splits = new GenericInputSplit[] {new GenericInputSplit(0)};
    return splits;
  }

  @Override
  public final Class<? extends GenericInputSplit> getInputSplitType() {
    return GenericInputSplit.class;
  }

  @Override
  public final void open(GenericInputSplit split) throws IOException {
    // Nothing to do here
  }
  
  @Override
  public final boolean nextRecord(Record record) throws IOException {
    ++ recordsProcessed;
    fillNextRecord(record, recordsProcessed);
    return true;
  }
  
  @Override
  public final boolean reachedEnd() throws IOException {
    return (recordsProcessed >= getNumRecords());
  }

  @Override
  public final void close() throws IOException {
    // Nothing to do here
  }

}
