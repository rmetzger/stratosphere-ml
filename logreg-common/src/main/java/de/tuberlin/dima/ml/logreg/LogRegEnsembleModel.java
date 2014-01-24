package de.tuberlin.dima.ml.logreg;

import java.util.List;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseRowMatrix;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.ClassificationModel;
import de.tuberlin.dima.ml.RegressionModel;
import de.tuberlin.dima.ml.logreg.LogRegMath;
import de.tuberlin.dima.ml.util.MLUtils;

/**
 * Implementation of a ensemble model for Logistic Regression
 * Internally it has multiple logistic regression models
 * Those are combined to a single prediction (supporting different VotingSchemas)
 */
public class LogRegEnsembleModel implements ClassificationModel, RegressionModel {
  
  List<Vector> models;
  double threshold;
  VotingSchema votingSchema;
  Vector wMergedModel;   // only used if votingSchema is merge_model
  
  public enum VotingSchema {
    MAJORITY_VOTE,
    MERGED_MODEL
  }

  public LogRegEnsembleModel(List<Vector> models, double threshold, VotingSchema votingSchema) {
    this.models = models;
    this.threshold = threshold;
    this.votingSchema = votingSchema;
    
    if (votingSchema == VotingSchema.MERGED_MODEL) {
      // TODO Interesting: How much vary the features between the models? How much sparsity is removed on merging?
      
      Vector[] modelArray = models.toArray(new Vector[models.size()]);
      Matrix modelsMatrix = new SparseRowMatrix(models.size(), models.get(0).size(), modelArray);
      wMergedModel = MLUtils.meanByColumns(modelsMatrix);
    }
  }

  @Override
  public double predict(Vector x) {
    return predict(x, 0);
  }

  @Override
  public double predict(Vector x, double intercept) {
    
    double prediction = -1;
    
    switch (votingSchema) {
    case MAJORITY_VOTE:
      
      double[] votes = new double[2];
      for (Vector w : models) {
        votes[ LogRegMath.classify(x, w) ] += LogRegMath.predict(x, w); 
      }
      // TODO Show how the variance is between the different models
      // TODO Show warning if number is even (no majority might exist)
      prediction = votes[1] > votes[0] ? 1 : 0;
      
      break;
      
    case MERGED_MODEL:

      prediction = LogRegMath.predict(x, wMergedModel, intercept);
      
      break;
      
    default:
      break;
    }
    
    return prediction;
  }

  @Override
  public int classify(Vector x) {
    return (int)Math.floor(predict(x) + threshold);
  }

}
