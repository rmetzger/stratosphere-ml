package de.tuberlin.dima.ml.logreg.sfo;

import java.util.List;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.logreg.LogRegModel;
import de.tuberlin.dima.ml.logreg.sfo.IncrementalModelOld;
import de.tuberlin.dima.ml.logreg.sfo.LogRegSFOTraining;
import de.tuberlin.dima.ml.util.CsvReader;
import de.tuberlin.dima.ml.util.MLUtils;
import de.tuberlin.dima.ml.validation.Validation;

/**
 * Sequential version of SFO algorithm (outdated)
 * 
 * Works with a csv input, internally represents data as Matrix
 */
public class SFOSequential {
  
  private CsvReader csvTrain;
  private CsvReader csvTest;
  IncrementalModelOld model;
  
  private static final double BIAS_DEFAULT = 1;
  private static final String TARGET_NAME = "color";
  private static final double TARGET_POSITIVE = 2d;
  private static final double TARGET_NEGATIVE = 1d;
  
  // Training Parameters
  private static final int ITERATIONS = 5;
  private static final double INITIAL_WEIGHT = 1d;
//  private static final double PENALTY = 1d;
  private static final double PENALTY = 0d; // No regularization
  
  public SFOSequential(String trainingFile, String testFile, List<String> predictorNames) throws Exception {
    
    csvTrain = MLUtils.readData(trainingFile, 40, predictorNames, TARGET_NAME, true);
    csvTrain.normalize();
    csvTrain.normalizeClassLabels(TARGET_POSITIVE, TARGET_NEGATIVE);
    csvTest = MLUtils.readData(testFile, 40, predictorNames, TARGET_NAME, true);
    csvTest.normalize(csvTrain.getMeans(), csvTrain.getRanges());
    csvTest.normalizeClassLabels(TARGET_POSITIVE, TARGET_NEGATIVE);
    
    // Initiate an empty model, just with bias
    model = new IncrementalModelOld(BIAS_DEFAULT, csvTrain.getNumPredictors());
  }
  
  public void findBestFeature() {
    
    System.out.println("-------------------");
    System.out.println("SFO Iteration");
    System.out.println("-------------------");
    
    Validation val = new Validation();
    
    // Measure performance of current base model
    Matrix XTestBase = transformMatrix(model.getUsedDimensions(), -1, csvTest.getData());
    LogRegModel logRegModel = new LogRegModel(model.getW());
    val.computeAccuracy(XTestBase, csvTest.getY(), logRegModel);
    val.computeMeanDeviation(XTestBase, csvTest.getY(), logRegModel);
    System.out.println("Base Model performance");
    System.out.println(" - dev:     " + val.getMeanDeviation());
    System.out.println(" - success: " + val.getAccuracy());
    double baseMeanDeviation = val.getMeanDeviation();

    // TODO Optimization: Always keep an extended Matrix, where the last row can be exchanged (much faster!)
    int bestDimension = 0;
    double bestGain = Double.NEGATIVE_INFINITY;
    double bestWeight = 0;
    double gain = Double.NEGATIVE_INFINITY;
    LogRegModel extendedModel = new LogRegModel(null);
    for (int d : model.getUnusedDimensions()) {
      
      // Get copy of matrix only with relevant dimensions
      Matrix XTrain = transformMatrix(model.getUsedDimensions(), d, csvTrain.getData());
      Matrix XTest = transformMatrix(model.getUsedDimensions(), d, csvTest.getData());
      
      // Train single feature
      System.out.println("Train " + csvTrain.getColumnName(d) + " (d=" + d + ")");
      Vector extendedW = model.getExtendedModel();     // weight vector with place for one more feature
      extendedW = LogRegSFOTraining.trainNewtonSFO(XTrain, csvTrain.getY(), extendedW, ITERATIONS, INITIAL_WEIGHT, PENALTY, (d==1 ? true : false));
      System.out.println(" - Trained beta_d:  " + extendedW.get(extendedW.size()-1));

      // Measure performance when adding dimension d to base model
      extendedModel.setW(extendedW);
      val.computeAccuracy(XTest, csvTest.getY(), extendedModel);
      val.computeMeanDeviation(XTest, csvTest.getY(), extendedModel);
      System.out.println(" - dev:     " + val.getMeanDeviation());
      System.out.println(" - success: " + val.getAccuracy());
      
      // Compute gain
      gain = baseMeanDeviation - val.getMeanDeviation();
      if (gain > bestGain) {
        bestDimension = d;
        bestGain = gain;
        bestWeight = extendedW.getQuick(extendedW.size() - 1);
        System.out.println(" -> New best dimension with gain " + gain);
      }
    }
    System.out.println("=> Best dimension: " + csvTrain.getColumnName(bestDimension) + " (d=" + bestDimension + ")\n");
    
    model.addDimensionToModel(bestDimension, bestWeight);
  }
  
  /**
   * Returns a copy of the input matrix with only relevant columns/dimensions in given order
   * Columns of input:          1 2 3 ... 10
   * Relevant dimensions:       4 2 5
   * Returned matrix columns:   4 2 5
   */
  private Matrix transformMatrix(List<Integer> relevantDimensions, int newDimension, Matrix input) {
    int numColumns = relevantDimensions.size() + (newDimension != -1 ? 1 : 0);
    Matrix copy = new DenseMatrix(csvTrain.getData().rowSize(), numColumns);
    for (int c=0; c<relevantDimensions.size(); ++c) {
      copy.assignColumn(c, input.viewColumn(relevantDimensions.get(c)));
    }
    if (newDimension != -1) {
      // Add the single new dimension
      copy.assignColumn(copy.numCols()-1, input.viewColumn(newDimension));
    }
    return copy;
  }

}
