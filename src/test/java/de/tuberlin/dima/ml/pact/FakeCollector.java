package de.tuberlin.dima.ml.pact;
import java.util.List;

import com.google.common.collect.Lists;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.type.PactRecord;

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
 * @param <T> Type of the Collector (usually PactRecords
 */
public class FakeCollector implements Collector<PactRecord> {
  
  List<PactRecord> recordsCollected = Lists.newArrayList();

  @Override
  public void collect(PactRecord record) {
    recordsCollected.add(record);
  }

  @Override
  public void close() {
    // Do nothing here
  }
  
  public List<PactRecord> getRecordsCollected() {
    return recordsCollected;
  }

}
