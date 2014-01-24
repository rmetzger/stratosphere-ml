package de.tuberlin.dima.ml.pact.logreg.ensemble;

import java.util.Random;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.ShortValue;
import eu.stratosphere.types.StringValue;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;



public class MapRandomPartitioning extends MapFunction {
  
  public static final int IDX_INPUT_LINE = 0;
  
  static final String CONF_KEY_NUM_FEATURES = "parameter.NUM_FEATURES";
  static final String CONF_KEY_NUM_PARTITIONS = "parameter.NUM_PARTITIONS";
  
  Random random = new Random();

  private int numPartitions;
  private int numFeatures;

  private final Record outputRecord = new Record();
  private final IntValue outputPartition = new IntValue();
  private final PactVector outputVector = new PactVector();
  private final ShortValue outputLabel = new ShortValue();
  
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
  public void map(Record record, Collector<Record> out)
      throws Exception {
    // TextInputFormat puts line into first field (as type StringValue)
    StringValue line = record.getField(IDX_INPUT_LINE, StringValue.class);
    
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