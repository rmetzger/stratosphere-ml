package de.tuberlin.dima.ml.pact.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.generic.io.InputFormat;

/**
 * See related {@link SingleValueDataSource} for explanation.
 */
public class SingleValueInputFormat implements
    InputFormat<PactRecord, GenericInputSplit> {

  // ------------------------------------- Config Keys ------------------------------------------

  public static final String CONF_KEY_VALUE_CLASS = "single-value-input-format.value-class";
  private Class<? extends Value> valueClass;
  
  public static final String CONF_KEY_FILE_PATH = "single-value-input-format.file-path";
  private String filePath;

  // ------------------------------------- SCHEMA -----------------------------------------------
  
  public static final int IDX_OUT_VALUE = 0;

  // --------------------------------------------------------------------------------------------
  
  private boolean reachedEnd = false;

  // --------------------------------------------------------------------------------------------
  
  @SuppressWarnings("unchecked")
  @Override
  public void configure(Configuration parameters) {
    String valueClassName = parameters.getString(CONF_KEY_VALUE_CLASS, "");
    if (valueClassName.isEmpty()) {
        throw new IllegalArgumentException("Please specify the type of the value (must implement Value interface)");
    }
    try {
      this.valueClass = (Class<? extends Value>) Class.forName(valueClassName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    
    this.filePath = parameters.getString(CONF_KEY_FILE_PATH, "");
    if (this.filePath.isEmpty()) {
        throw new IllegalArgumentException("Please specify the path of file containing the serialized value");
    }
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
      throws IOException {
    return new BaseStatistics() {
      @Override
      public long getTotalInputSize() {
        return BaseStatistics.SIZE_UNKNOWN;
      }
      @Override
      public long getNumberOfRecords() {
//        return BaseStatistics.NUM_RECORDS_UNKNOWN;
        return 1;
      }
      @Override
      public float getAverageRecordWidth() {
        return BaseStatistics.AVG_RECORD_BYTES_UNKNOWN;
      }
    };
  }

  @Override
  public GenericInputSplit[] createInputSplits(int minNumSplits)
      throws IOException {
    // return a single split
    GenericInputSplit[] splits = new GenericInputSplit[] {new GenericInputSplit(0)};
    return splits;
  }

  @Override
  public Class<? extends GenericInputSplit> getInputSplitType() {
    return GenericInputSplit.class;
  }

  @Override
  public void open(GenericInputSplit split) throws IOException {
    // Nothing to do here
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return reachedEnd;
  }

  @Override
  public boolean nextRecord(PactRecord record) throws IOException {
    Value value = null;
    try {
      value = valueClass.newInstance();
    } catch (InstantiationException e) {
      e.printStackTrace(); return false;
    } catch (IllegalAccessException e) {
      e.printStackTrace(); return false;
    }
    
    URI filePathUri = null;
    try {
      filePathUri = new URI(filePath);
    } catch (URISyntaxException e) {
      e.printStackTrace(); return false;
    }
    FileSystem fs = FileSystem.get(filePathUri);
    FSDataInputStream stream = fs.open(new Path(filePath));
    DataInputStream inStream = new DataInputStream(stream);
    
    value.read(inStream);
    
    record.setField(IDX_OUT_VALUE, value);
    
    stream.close();
    
    reachedEnd = true;
    return true;
  }

  @Override
  public void close() throws IOException {
    // Nothing to do here
  }

}
