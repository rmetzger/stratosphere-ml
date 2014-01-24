package de.tuberlin.dima.ml;

import org.apache.mahout.math.Vector;

/**
 * Interface for a model for classification, i.e. a model that predicts a
 * discrete value (the class) for a given input vector.
 * 
 * @author André Hacker
 * 
 */
public interface ClassificationModel {
  
  /**
   * Classify x using the current model. If the underlying model outputs a
   * probability, the result depends on an threshold (typically 0.5)
   */
  int classify(Vector x);

}
