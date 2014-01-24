package de.tuberlin.dima.ml.validation;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.ClassificationModel;
import de.tuberlin.dima.ml.RegressionModel;

/**
 * Several Validation methods for Matrix based implementations
 * Pretty outdated...
 */
public class Validation {  
  
  Matrix confusion;
  double accuracy;
  double meanDeviation;
  
  // TODO Refactoring: Redundant. Merge this with computeSuccessRate etc.
  public void computeMetrics(Matrix testData, Vector testY, RegressionModel regressionModel, ClassificationModel classificationModel) {
    // How many do we classify correctly?
    
    // TODO Feature: Compute AUC
    confusion = new DenseMatrix(2,2);
    int truePos = 0;
    int trueNeg = 0;
    int falsePos = 0;
    int falseNeg = 0;
    double dev = 0;     // deviation
    for (int n=0; n<testData.numRows(); ++n) {
      //System.out.println(data.viewRow(n));
      double predictedClass = classificationModel.classify(testData.viewRow(n));
//      System.out.println("Is: " + prediction + " should: " + y.get(n));
      if (Math.round(predictedClass) == testY.get(n)) {
        if (testY.get(n) == 0)
          ++truePos;
        else
          ++trueNeg;
      } else {
        if (testY.get(n) == 0)
          ++falseNeg;
        else
          ++falsePos;
      }

      double prediction = regressionModel.predict(testData.viewRow(n));
      dev += Math.abs((testY.get(n) - prediction));
    }
    confusion.set(0, 0, truePos);
    confusion.set(0, 1, falseNeg);
    confusion.set(1, 0, falsePos);
    confusion.set(1, 1, trueNeg);
    accuracy = ((double)truePos + trueNeg) / ((double)testData.numRows());
    meanDeviation = dev / testData.numRows();
  }
  
  public void computeMeanDeviation(Matrix data, Vector y, RegressionModel model) {
    double dev = 0;
    for (int n=0; n<data.numRows(); ++n) {
      //System.out.println(data.viewRow(n));
      double prediction = model.predict(data.viewRow(n));
      dev += Math.abs((y.get(n) - prediction));
    
    //    System.out.println("Is: " + prediction + " should: " + y.get(n));
    }
    this.meanDeviation = dev / data.numRows(); 
  }

  public void computeAccuracy(Matrix data, Vector y, ClassificationModel model) {
    // How many do we classify correctly?
    confusion = new DenseMatrix(2,2);
    int truePos = 0;
    int trueNeg = 0;
    int falsePos = 0;
    int falseNeg = 0;
    for (int n=0; n<data.numRows(); ++n) {
      //System.out.println(data.viewRow(n));
      double prediction = model.classify(data.viewRow(n));
  //    System.out.println("Is: " + prediction + " should: " + y.get(n));
      if (Math.round(prediction) == y.get(n)) {
        if (y.get(n) == 0)
          ++truePos;
        else
          ++trueNeg;
      } else {
        if (y.get(n) == 0)
          ++falseNeg;
        else
          ++falsePos;
      }
    }
    confusion.set(0, 0, truePos);
    confusion.set(0, 1, falseNeg);
    confusion.set(1, 0, falsePos);
    confusion.set(1, 1, trueNeg);
    this.accuracy = ((double)truePos + trueNeg) / ((double)data.numRows()); 
  }
  
  public Matrix getConfusion() {
    return confusion;
  }
  
  public double getMeanDeviation() {
    return meanDeviation;
  }
  
  public double getAccuracy() {
    return accuracy;
  }

}
