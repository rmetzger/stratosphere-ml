package de.tuberlin.dima.ml.logreg;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.util.MLUtils;

/**
 * Static methods for Logistic Regression
 * 
 * TODO Feature: L2-Regularization (simple)
 * TODO Feature: L1-Regularization (see mahout sgd or paper with differential L1 approximation)
 */
public class LogRegMath {
  
  private LogRegMath() { }

  public static double predict(Vector x, Vector w) {
    return logisticFunction(x.dot(w));
  }
  
  public static double predict(Vector x, Vector w, double intercept) {
    return logisticFunction(x.dot(w) + intercept);
  }
  
  /**
   * Classifies x using the given threshold (usually 0.5)
   * @return 1 = positive class, 0 = negative class
   */
  public static int classify(Vector x, Vector w, double intercept, double threshold) {
    double prediction = predict(x, w, intercept);
    return (int) Math.floor(prediction + threshold);
  }

  public static int classify(Vector x, Vector w) {
    return classify(x, w, 0, 0.5);
  }
  
  /**
   * Computes the prediction, using our current hypothesis (logistic function)
   * 
   * Handles numerical issues (actually we ignore them because they don't
   * disturb here)
   * 
   * - Underflows: The real result is something very close to zero, but due to
   * the lack of precision we get 0.0. In the logistic function this does not
   * cause any problems so we just ignore it
   * 
   * - Overflows: We should get a very big number, but due to the lack of
   * precision we get Double.POSITIVE_INFINITY instead. In the logistic function
   * this does not cause any problems, because java will actually calculate with
   * INFINITY as we would expect it.
   * 
   * - NaN: This occurs only when dividing by zero, which is not the case here
   */
  public static double logisticFunction(double exp) {
    
    double negativeExpResult = Math.exp(-exp);
    
    // Overflow detection
//    if (Double.isInfinite(negativeExpResult) || Double.isNaN(negativeExpResult)) {
//      System.out.println("OVERFLOW (ignored): exp=" + exp + " e^(-exp)=" + negativeExpResult);
//    }
    
    // Underflow detection (no problem, just a lack of precision)
//    if (exp != 0 && negativeExpResult == 0) {
//      System.out.println("UNDERFLOW (ignored): exp=" + exp + " e^(-exp)=" + negativeExpResult);
//    }
    
    return 1d / (1d + negativeExpResult);
  }

  /**
   * Computes the log-likelihood for a single data point
   * Uses the natural logarithm
   * 
   * TODO Handle special numerical cases: 0.0, INFINITY, NaN
   * 
   * See Hastie p.120 (equation 4.20) for explanation
   */
  public static double logLikelihood(double actual, double prediction) {
    if (prediction == 0) {
      System.out.println("logLikelihood: prediction == 0 detected and solved (would result in log(0)). prediction=" + prediction + " actual=" + actual);
      prediction = Double.MIN_VALUE;
    }
    if (prediction == 1) {
      System.out.println("logLikelihood: prediction == 1 detected and solved (would result in log(0)). prediction=" + prediction + " actual=" + actual);
      prediction = 0.9999999D;
    }
//    if (prediction < 0) {
//      System.out.println("logLikelihood: log(<0) detected and solved. prediction=" + prediction + " actual=" + actual);
//      prediction = Double.MIN_VALUE;
//    }
//    if (prediction == Double.POSITIVE_INFINITY) {
//      System.out.println("logLikelihood: prediction=positive infty detected and solved. prediction=" + prediction + " actual=" + actual);
//      prediction = Double.MAX_VALUE;
//    }
//    if (prediction == Double.NaN) {
//      System.out.println("logLikelihood: prediction=NaN detected and solved. prediction=" + prediction + " actual=" + actual);
//      prediction = Double.MAX_VALUE;
//    }
//    return (actual * Math.log(prediction)) + ((1 - actual) * (1 - Math.log(prediction)));
    return (actual * Math.log(prediction)) + ((1 - actual) * Math.log((1 - prediction)));
  }
  
  public static void main(String[] args) {
	double actual = 1;
	double prediction = Double.MIN_VALUE;
	System.out.println((actual * Math.log(prediction)) + ((1 - actual) * Math.log((1 - prediction))));
  }

  public static double computeSqError(Vector x, Vector w, double y) {
    return Math.pow(predict(x, w) - y, 2);
  }

  /**
   * Compute the partial gradient of negative log-likelihood function regarding a single data point x
   * = ( h(x) - y) * x
   */
  public static Vector computePartialGradient(Vector x, Vector w, double y) {
    return x.times(predict(x, w) - y);
  }

  public static Matrix computeSecondPartialGradient(Vector x, Vector w, double y) {
    //Returns: x x^T h(x) (1-h(x))
    double predicted = LogRegMath.predict(x, w);
    Matrix productOfx = MLUtils.vectorToColumnMatrix(x).times(MLUtils.vectorToRowMatrix(x));
    
    Matrix result = productOfx.times(predicted * (1 - predicted));
    return result;
  }

}
