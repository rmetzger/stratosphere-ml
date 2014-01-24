package de.tuberlin.dima.ml;

import org.apache.mahout.math.Vector;

/**
 * Interface for a model for regression, i.e. a model that predicts a real value
 * for a given input vector.
 * 
 * @author André Hacker
 * 
 */
public interface RegressionModel {

  /**
   * Predict the outcome for input x
   * 
   * Either does not use bias/interception term or x and w both have a dimension for bias included
   */
  double predict(Vector x);

  /**
   * Predict the outcome for input x using an explicit bias / interception term 
   */
  double predict(Vector x, double intercept);

}
