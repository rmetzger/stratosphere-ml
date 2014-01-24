package de.tuberlin.dima.ml.logreg;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

import de.tuberlin.dima.ml.util.MLUtils;

/**
 * Static methods for training of a Logistic Regression Model
 * Supports various different training methods
 */
public class LogRegTraining {
  
  private LogRegTraining() { }
  
  public static Vector trainBatchGD(Matrix data, Vector y, int maxIterations, double learningRate, double initialWeight) {

    Vector w = new DenseVector(data.numCols());
    w.assign(initialWeight);
    int rowCount = data.numRows();
    
    int it = 0;
    while ((++it) <= maxIterations) {
      
      Vector batchGradient = new DenseVector(data.numCols());
      // Batch GD: Iterate over all x
      for (int n=0; n<rowCount; ++n) {
        Vector grad = LogRegMath.computePartialGradient(data.viewRow(n), w, y.get(n));
        batchGradient.assign(grad, Functions.PLUS);
      }
      // Weight update: w = w - 1/N * \gamma * grad
        w.assign(batchGradient.assign(Functions.MULT, learningRate / rowCount), Functions.MINUS);
    }
    return w;
  }

  public static Vector trainNewton(Matrix data, Vector y, int maxIterations, double initialWeight, double penalty) {
    
    Vector w = new DenseVector(data.numCols());
    w.assign(initialWeight);

    int rowCount = data.numRows();
    
    double penaltyDivN = penalty/rowCount;

    int it = 0;
    // Other termination criteria: small delta in weights or good quality reached
    while ((++it) <= maxIterations) {
      Vector batchGradient = new DenseVector(data.numCols());
      Matrix batchGradientSecond = new DenseMatrix(data.numCols(), data.numCols());
      // Batch GD: Iterate over all x
      for (int n=0; n<rowCount; ++n) {
        Vector xn = data.viewRow(n);
        Vector grad = LogRegMath.computePartialGradient(xn, w, y.get(n));
        batchGradientSecond.assign(LogRegMath.computeSecondPartialGradient(xn, w, y.get(n)), Functions.PLUS);
        batchGradient.assign(grad, Functions.PLUS);
      }
      // Add penalty to 1st derivation (using NG's derivation)
      Vector mask = MLUtils.ones(data.numCols()); // Avoid penalty on bias
      mask.set(0, 0);
      batchGradient.assign(w.times(mask).times(penaltyDivN), Functions.PLUS);
      
      // Add penalty to 2nd derivation
      batchGradientSecond = batchGradientSecond.plus(MLUtils.diag(mask).times(penaltyDivN));
      
      // Standard Newton Update
      batchGradient = MLUtils.inverse(batchGradientSecond).times(batchGradient);
      w.assign(batchGradient, Functions.MINUS);
    }
    
    return w;
  }
  
}
