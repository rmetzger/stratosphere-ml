package de.tuberlin.dima.ml.linreg;

import java.io.BufferedReader;
import java.util.List;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;


import de.tuberlin.dima.ml.ClassificationModel;
import de.tuberlin.dima.ml.RegressionModel;
import de.tuberlin.dima.ml.util.CsvReader;
import de.tuberlin.dima.ml.util.MLUtils;
import de.tuberlin.dima.ml.validation.Validation;

public class LinearRegression implements RegressionModel, ClassificationModel {
  
  Vector w = null;
  
  public void train(String inputFile, List<String> predictorNames) throws Exception {
    BufferedReader reader = new BufferedReader(MLUtils.open(inputFile));
    
    // Read numeric csv into dense matrix
    CsvReader csv = new CsvReader();
    int rows = 40;
    csv.numericToDenseMatrix(reader, rows, "color", predictorNames, true);
    Matrix data = csv.getData();
    
    // Least squares solution:
    w = MLUtils.pseudoInversebySVD(data).times(csv.getY());
    System.out.println("Learned weigths: " + w);

    Validation val = new Validation();
    val.computeMeanDeviation(data, csv.getY(), this);
    val.computeAccuracy(data, csv.getY(), this);
    System.out.println("Mean Deviation: " + val.getMeanDeviation());
    System.out.println("Success-rate: " + val.getAccuracy());
  }
  
  public double predict(Vector x) {
    return x.dot(w);
  }
  
  public double predict(Vector x, double intercept) {
    return x.dot(w) + intercept;
  }

  public int classify(Vector x) {
    return (int) Math.round( predict(x) );
  }
  
}
