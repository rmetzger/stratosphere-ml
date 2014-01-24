package de.tuberlin.dima.ml.validation;

/**
 * Computes accuracy and metrics related to confusion matrix 
 * 
 * Inspired by Mahouts OnlineAuc class
 */
public class OnlineAccuracy {
    
  /**
   * Confusion matrix:
   *           Prediction
   *             T      N
   * Actual T |  TP  |  FN
   *        N |  FP  |  TN 
   */
  
  private double threshold;
  
  private long truePositives=0;
  private long trueNegatives=0;
  private long positives=0;
  private long negatives=0;
  
  public OnlineAccuracy(double threshold) {
    this.threshold = threshold;
  }
  
  /**
   * @param actualTarget assumed to be 0 or 1
   * @param prediction
   */
  public void addSample(int actualTarget, double prediction) {
    long predictedClass = (long)Math.floor(prediction + threshold);
    if (predictedClass == actualTarget) {
      if (actualTarget == 0)
        ++truePositives;
      else
        ++trueNegatives;
    }

    if (actualTarget == 0)
      ++positives;
    else
      ++negatives;
  }
  
  public double getAccuracy() {
    return (double)(truePositives + trueNegatives) / (double)(negatives + positives);
  }
  
  public long getPositives() {
    return positives;
  }
  
  public long getNegatives() {
    return negatives;
  }
  
  public long getTrueNegatives() {
    return trueNegatives;
  }
  
  public long getTruePositives() {
    return truePositives;
  }
  
  public double getThreshold() {
    return threshold;
  }

  public long getTotal() {
    return positives + negatives;
  }

  public long getCorrect() {
    return truePositives + trueNegatives;
  }

}
