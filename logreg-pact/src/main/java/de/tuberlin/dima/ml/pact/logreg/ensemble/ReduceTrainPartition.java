package de.tuberlin.dima.ml.pact.logreg.ensemble;

import java.util.Iterator;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.ShortValue;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.pact.types.PactVector;
import de.tuberlin.dima.ml.validation.OnlineAccuracy;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;


public class ReduceTrainPartition extends ReduceFunction {
  
  public static final int IDX_PARTITION = 0;
  public static final int IDX_VECTOR = 1;
  public static final int IDX_LABEL = 2;
  
  static final String CONF_KEY_NUM_FEATURES = "parameter.NUM_FEATURES";
  private int numFeatures;
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    numFeatures = parameters.getInteger(CONF_KEY_NUM_FEATURES, 0);
  }

  @Override
  public void reduce(Iterator<Record> records, Collector<Record> out)
      throws Exception {
    System.out.println("REDUCER");
    
    // TODO Try AdaptiveLogisticRegression
    boolean useAdaptive = false;
    OnlineLogisticRegression learningAlgorithm = null;
    if (useAdaptive) {
      // TODO Use Adaptive Logistic Regression.
//      learningAlgorithm = new AdaptiveLogisticRegression(
//          2, 
//          numFeatures,
//          new L1());
    } else {
      learningAlgorithm = new OnlineLogisticRegression(
          2, 
          numFeatures, 
          new L1());
      learningAlgorithm.alpha(1) // 1 (skipping is bad)
          .stepOffset(1000) // 1000
          .decayExponent(0.1) // 0.9
          .lambda(3.0e-6) // 3.0e-5
          .learningRate(15); // 20
    }

    OnlineAccuracy accuracy = new OnlineAccuracy(0.5);
    Record element = null;
    Vector vec = null;
    int count = 0;
    while (records.hasNext()) {

      element = records.next();
      vec = element.getField(IDX_VECTOR, PactVector.class).getValue();
      short actualTarget = element.getField(IDX_LABEL, ShortValue.class).getValue();
      
      // Test prediction
      double prediction = learningAlgorithm.classifyScalar(vec);
      accuracy.addSample(actualTarget, prediction);

      // Train
      learningAlgorithm.train(actualTarget, vec);
      
      ++count;
    }
    
    Vector w = learningAlgorithm.getBeta().viewRow(0);    // Returned vector is dense (which is good so)
    
    int partition = element.getField(IDX_PARTITION, IntValue.class).getValue();
    System.out.println("- partition: " + partition);
    System.out.println("- count: " + count);
    System.out.println("- non zeros: " + w.getNumNonZeroElements());
    System.out.println("- ACCURACY (online, in-sample): " + accuracy.getAccuracy() + " (= " + (accuracy.getTrueNegatives() + accuracy.getTruePositives()) + " / " + accuracy.getTotal() + ")");
    learningAlgorithm.close();
    
    Record outputRecord = new Record();
    outputRecord.setField(ReduceFlattenModel.IDX_MODEL_ID, new IntValue(1));
    outputRecord.setField(ReduceFlattenModel.IDX_PARTITION, new IntValue(partition));
    outputRecord.setField(ReduceFlattenModel.IDX_MODEL, new PactVector(w));
    out.collect(outputRecord);
  }
}