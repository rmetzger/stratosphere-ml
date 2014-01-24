package de.tuberlin.dima.ml.pact;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.List;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.pact.types.PactVector;
import de.tuberlin.dima.ml.pact.udfs.ReduceFlattenToVector;
import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.configuration.Configuration;


public class ReduceFlattenToVectorTest {
  
  @Mock Configuration emptyConfiguration;
  @Mock Configuration configuration;
  
  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void test() throws Exception {
    int size = 10;
    List<Record> records = makeTestData(size);
    ReduceFunction udf = new ReduceFlattenToVector();
    FakeCollector fakeCollector = new FakeCollector();
    
    // configure udf
    testSetInteger(configuration, ReduceFlattenToVector.CONF_KEY_NUM_FEATURES, size);
    udf.open(configuration);
    
    // Assumed output: 1 record with 2 fields (key and vector)
    udf.reduce(records.iterator(), fakeCollector);
    assertTrue(fakeCollector.getRecordsCollected().size() == 1);
    Record first = fakeCollector.getRecordsCollected().get(0);
    assertEquals(first.getNumFields(), 2);
    int key = first.getField(ReduceFlattenToVector.IDX_OUT_KEY_CONST_ONE, IntValue.class).getValue();
    assertEquals(key, 1);
    Vector vector = first.getField(ReduceFlattenToVector.IDX_OUT_VECTOR, PactVector.class).getValue();
    assertEquals(vector.size(), size);
    assertEquals(vector.get(3), 3, 0);  // test sample
  }
  
  @Test(expected=IllegalStateException.class)
  public void testNoParameter() throws Exception {
    ReduceFunction udf = new ReduceFlattenToVector();
    // Should throw exception now (see annotation)
    udf.open(emptyConfiguration);
  }
  
  private List<Record> makeTestData(int size) {
    List<Record> list = Lists.newArrayList();
    for (int i=0; i<size; ++i) {
      Record record = new Record(3);
      record.setField(ReduceFlattenToVector.IDX_KEY_CONST_ONE, PactUtils.pactOne);
      record.setField(ReduceFlattenToVector.IDX_DIMENSION, new IntValue(i));
      record.setField(ReduceFlattenToVector.IDX_DOUBLE_VALUE, new DoubleValue(i));
      list.add(record);
    }
    return list;
  }
  
  private void testSetInteger(Configuration conf, String key, int value) {
    when(configuration.getInteger(Matchers.matches(key), anyInt())).thenReturn(value);
  }

}
