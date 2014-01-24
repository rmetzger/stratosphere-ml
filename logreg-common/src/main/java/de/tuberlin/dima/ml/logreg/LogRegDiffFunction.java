package de.tuberlin.dima.ml.logreg;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

import edu.stanford.nlp.optimization.DiffFunction;

/**
 * Required for stanford-nlp L-BFGS Optimizer
 * (package edu.stanford.nlp.optimization)
 */
public class LogRegDiffFunction implements DiffFunction {

    private final Matrix input;
    private final Vector labels;
    private int countValueAt = 0;
    private int countDeriveAt = 0;

    public LogRegDiffFunction(Matrix input, Vector labels) {
        this.input = input;
        this.labels = labels;
    }

    @Override
    public int domainDimension() {
        return this.input.numCols();
    }

    @Override
    // In-sample error as cost function
    public double valueAt(double[] weight) {

        Vector w = new DenseVector(weight);
        double trainingError = 0.0;

        // \sum_{i=0}^{N} ln(1 + e^{-y_i * w^T x_i})
        for (int i = 0; i < this.input.numRows(); i++) {

            Vector x = this.input.viewRow(i);
            double score = x.dot(w);
            double y = this.labels.get(i);

            trainingError += Math.log(1.0 + Math.exp(-y * score));
        }

        ++countValueAt;
        return trainingError;
    }

    @Override
    public double[] derivativeAt(double[] weight) {
        
        Vector w = new DenseVector(weight);
        Vector gradient = new DenseVector(weight.length);
        
        // Add partial gradients
        // Note: this can be refactored with the existing gradient computation
        // from LogisticRegression, but I wanted to keep it independent for now
        for (int i = 0; i < this.input.numRows(); i++) {
            Vector x = this.input.viewRow(i);
            double y = this.labels.get(i);
            
            double prediction = 1.0/(1.0 + Math.exp(-x.dot(w)));
            double diff = prediction - y;
            
            Vector partial = x.times(diff);
            
            gradient.assign(partial, Functions.PLUS);
        }
        
        // Vector to double array
        double[] d = new double[weight.length];
        for (int i = 0; i < gradient.size(); i++) {
            d[i] = gradient.get(i);
        }

        ++ countDeriveAt;
        return d;
    }
    
    public int getCountDeriveAt() {
      return countDeriveAt;
    }
    
    public int getCountValueAt() {
      return countValueAt;
    }
}