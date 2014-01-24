package de.tuberlin.dima.ml.logreg.sfo;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.logreg.LogRegMath;

/**
 * Old (outdated) class.
 * Contains training methods for a sequential SFO implementation
 * 
 * @author André Hacker
 */
public class LogRegSFOTraining {
  
  public static Vector trainNewtonSFO(Matrix X, Vector y, Vector w, int maxIterations, double initialWeight, double penalty, boolean debug) {
    
    int rowCount = X.numRows();
    
    double penaltyDivN = penalty/rowCount;
    
    int it = 0;
    while ((++it) <= maxIterations) {
      
      double batchGradient = 0;
      double batchGradientSecond = 0;
      double update = 0;
      double pi=0;  // Current prediction
      // Batch GD: Iterate over all x
      double debugSumPi=0;
      for (int i=0; i<rowCount; ++i) {
        Vector xi = X.viewRow(i);
        //batchGradient += computePartialGradientSFO(xn, w, y.get(n));
        pi = LogRegMath.predict(xi, w);
        debugSumPi += pi;
        batchGradient += derivateSFO(xi.getQuick(xi.size()-1), pi, y.get(i));
        batchGradientSecond += derivateSecondSFO(xi.getQuick(xi.size()-1), pi);
      }
      
      // Apply Regularization to 1st derivation
      // using NG's derivation from http://openclassroom.stanford.edu/MainFolder/DocumentPage.php?course=MachineLearning&doc=exercises/ex5/ex5.html
      // equal to Singhs definition, except the division by n
      // Apply only to the the dimension under training
      batchGradient += penaltyDivN * w.getQuick(w.size()-1);
      
      // Apply regularization to 2nd derivation
      batchGradientSecond += penaltyDivN;
      
      // Standard Newton Update
      update = batchGradient / batchGradientSecond;
      w.setQuick(w.size()-1, w.getQuick(w.size()-1) - update);
      
      if (debug) {
        System.out.println("- it " + it + ": grad: " + batchGradient + " gradSecond: " + batchGradientSecond + " new betad: " + w.getQuick(w.size()-1) + " sumPi: " + debugSumPi);
      }
    }
    return w;
  }

  /**
   * Computes the gradient for the negative log-likelihood function regarding a
   * single data point x and keeping all dimensions except a single dimension
   * (d) constant<br>
   * 
   * Does not use Regularization<br>
   * 
   * This is the negative of the 2.8 in the paper (because we minimize
   * negative-ll here)<br>
   * 
   * = ( h_d(x_i) - y) * x_id
   */
  public static double derivateSFO(double xid, double pi, double y) {
    return (pi - y) * xid;
  }
  
  /**
   * Computes the second gradient for the negative log-likelihood function regarding a
   * single data point x and keeping all dimensions except a single dimension
   * (d) constant<br>
   * 
   * Does not use Regularization<br>
   * 
   * This is the negative of the 2.9 in the paper (because we minimize
   * negative-ll here)<br>
   * 
   * = (x_id)^2 * h_d(x_i) * (1 - h_d(x_i))
   */
  public static double derivateSecondSFO(double xid, double pi) {
    double xidSquared = Math.pow(xid, 2);
    return xidSquared * pi * (1 - pi);
  }

  /**
   * Similar to
   * {@link LogRegSFOTraining#derivateSFO(double, double, double)}
   * but uses L2 regularization<br>
   * 
   * = 2 * lambda * beta_d - derivate
   * See 2.11 in the paper.
   */
  public static double derivateL2SFO(double xid, double pi, double y, double lambda, double betad) {
    return 2 * lambda * betad - derivateSFO(xid, pi, y);
  }

  /**
   * Similar to
   * {@link LogRegSFOTraining#computeSecondPartialGradientSFO(double, double, double)}
   * but uses L2 regularization<br>
   * 
   * = 2 * lambda - secondDerivate
   * See 2.12 in the paper.
   */
  public static double derivateSecondL2SFO(double xid, double pi, double lambda) {
    return 2 * lambda - derivateSecondSFO(xid, pi);
  }

}
