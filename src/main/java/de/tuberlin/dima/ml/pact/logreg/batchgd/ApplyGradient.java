package de.tuberlin.dima.ml.pact.logreg.batchgd;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;

public class ApplyGradient extends CrossStub {
  
  public static final String CONF_KEY_LEARNING_RATE = "parameter.LEARNING_RATE";

  public static final int IDX_INPUT1_OLD_MODEL = 0;
  
  public static final int IDX_INPUT2_MODEL_KEY = 0;
  public static final int IDX_INPUT2_GRADIENT = 1;
  
  private double learningRate = 0;
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.learningRate = Double.parseDouble(parameters.getString(CONF_KEY_LEARNING_RATE, "1"));
  }

  @Override
  public void cross(PactRecord modelRecord, PactRecord gradientRecord,
      Collector<PactRecord> out) throws Exception {
    System.out.println("--------\nAPPLY GRADIENT\n--------");
    
    Vector w = modelRecord.getField(IDX_INPUT1_OLD_MODEL, PactVector.class).getValue();
    Vector gradient = gradientRecord.getField(IDX_INPUT2_GRADIENT, PactVector.class).getValue();
    
    System.out.println("- Old model: D=" + w.size() + " non-zeros=" + w.getNumNonZeroElements());
    System.out.println("- Gradient: D=" + gradient.size() + " non-zeros=" + gradient.getNumNonZeroElements());

    // TODO Apply different learning rates, and find out which performs best
    gradient.assign(Functions.MULT, learningRate);
    w.assign(gradient, Functions.MINUS);
    
    if (!w.isDense()) {
      System.out.println("- Converting model to dense vector");
      w = new DenseVector(w);
    }
    
    System.out.println("- New model: D=" + w.size() + " non-zeros=" + w.getNumNonZeroElements() + " is-dense=" + w.isDense() + " learningRate=" + learningRate);
    System.out.println("--------");

    PactRecord recordOut = new PactRecord(1);
    recordOut.setField(ComputeGradientParts.IDX_INPUT2_MODEL, new PactVector(w));
    out.collect(recordOut);
  }

}
