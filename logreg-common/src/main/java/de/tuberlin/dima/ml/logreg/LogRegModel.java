package de.tuberlin.dima.ml.logreg;

import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.ClassificationModel;
import de.tuberlin.dima.ml.RegressionModel;

/**
 * Implements a simple model for Logistic Regression
 * It is defined by a weight (and an interception term, currently unused) 
 */
public class LogRegModel implements RegressionModel, ClassificationModel {
  
  private Vector w;
  private double threshold;
  
  public LogRegModel(Vector w) {
    this(w, 0.5);
  }
  
  public LogRegModel(Vector w, double threshold) {
    this.w = w;
    this.threshold = threshold;
  }
  
  @Override
  public double predict(Vector x) {
    return LogRegMath.predict(x, w);
  }

  @Override
  public double predict(Vector x, double intercept) {
    return LogRegMath.predict(x, w, intercept);
  }

  @Override
  public int classify(Vector x) {
    return LogRegMath.classify(x, w, 0, threshold);
  }

  /**
   * Compute the partial gradient of negative log-likelihood function regarding a single data point x
   * Convenience method, also available via LogRegMath.computePartialGradient()
   */
  public Vector computePartialGradient(Vector x, double y) {
    return LogRegMath.computePartialGradient(x, w, y);
  }
  
  public void setW(Vector w) {
    this.w = w;
  }
  
  public Vector getW() {
    return w;
  }
  
  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }
  
  public double getThreshold() {
    return threshold;
  }

}
