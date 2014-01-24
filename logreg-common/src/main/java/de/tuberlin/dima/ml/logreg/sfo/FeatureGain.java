package de.tuberlin.dima.ml.logreg.sfo;

import com.google.common.primitives.Doubles;

/**
 * Wraps the gain and trained coefficient for a single feature (dimension). Will
 * be populated after the feature was evaluated.
 * 
 * The gain refers to the gain in any metric (e.g. log-likelihood) when adding
 * this feature to an existing model (base model)
 * 
 * @author André Hacker
 */
public class FeatureGain implements Comparable<FeatureGain> {
  private int dimension;
  private double gain;
  private double coefficient;

  public FeatureGain(int dimension, double gain) {
    this(dimension, gain, 0);
  }

  public FeatureGain(int dimension, double gain, double coefficient) {
    this.dimension = dimension;
    this.gain = gain;
    this.coefficient = coefficient;
  }

  @Override
  public int compareTo(FeatureGain other) {
    return Doubles.compare(this.gain, other.gain);
  }

  public int getDimension() {
    return dimension;
  }

  public double getGain() {
    return gain;
  }
  
  public double getCoefficient() {
    return coefficient;
  }
}
