package de.tuberlin.dima.ml.pact.logreg.ensemble;

import java.util.Random;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactShort;
import eu.stratosphere.pact.common.type.base.PactString;

public class MapRandomPartitioning extends MapStub {
  
  public static final int IDX_INPUT_LINE = 0;
  
  static final String CONF_KEY_NUM_FEATURES = "parameter.NUM_FEATURES";
  static final String CONF_KEY_NUM_PARTITIONS = "parameter.NUM_PARTITIONS";
  
  Random random = new Random();

  private int numPartitions;
  private int numFeatures;

  private final PactRecord outputRecord = new PactRecord();
  private final PactInteger outputPartition = new PactInteger();
  private final PactVector outputVector = new PactVector();
  private final PactShort outputLabel = new PactShort();
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    numPartitions = parameters.getInteger(CONF_KEY_NUM_PARTITIONS, 0);
    numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, 0);
    System.out.println("Prepare Map");
    System.out.println("- num partitions: " + numPartitions);
    System.out.println("- num features: " + numFeatures);
  }

  @Override
  public void map(PactRecord record, Collector<PactRecord> out)
      throws Exception {
    // TextInputFormat puts line into first field (as type PactString)
    PactString line = record.getField(IDX_INPUT_LINE, PactString.class);
    
    Vector v = new RandomAccessSparseVector(numFeatures);
    int label = LibSvmVectorReader.readVectorSingleLabel(v, line.getValue());
    
//    System.out.println(v.getNumNonZeroElements());
//    for (int i=1; i<=3; ++i) {
    outputPartition.setValue(random.nextInt(numPartitions));
    outputVector.setValue(v);
    outputLabel.setValue((short)label);
    outputRecord.setField(ReduceTrainPartition.IDX_PARTITION, outputPartition);
    outputRecord.setField(ReduceTrainPartition.IDX_VECTOR, outputVector);
    outputRecord.setField(ReduceTrainPartition.IDX_LABEL, outputLabel);
    out.collect(outputRecord);
//    }
  }
}