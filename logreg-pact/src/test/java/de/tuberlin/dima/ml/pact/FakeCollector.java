package de.tuberlin.dima.ml.pact;

import com.google.common.collect.Lists;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

import java.util.List;

/**
 * A fake collector that can be used in unit tests for UDFs. It collects all
 * emitted records in a list.<br/>
 * 
 * Fake is meant in the sense of Martin fowler: We implement the whole
 * interface, but implementations are very simple and just for purpose of
 * testing. See http://www.martinfowler.com/bliki/TestDouble.html
 * 
 * @author André Hacker
 * 
 */
public class FakeCollector implements Collector<Record> {
  
  List<Record> recordsCollected = Lists.newArrayList();

  @Override
  public void collect(Record record) {
    recordsCollected.add(record);
  }

  @Override
  public void close() {
    // Do nothing here
  }
  
  public List<Record> getRecordsCollected() {
    return recordsCollected;
  }

}
