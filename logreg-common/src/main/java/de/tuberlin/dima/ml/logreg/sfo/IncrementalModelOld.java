package de.tuberlin.dima.ml.logreg.sfo;

import java.util.List;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

/**
 * Keeps a model that can be extended feature by feature
 * Needed for forward feature selection.
 * Keeps track of the used features (base model) and the unused/remaining features
 * 
 * TODO Maybe I don't need unusedDimensions at all, because they are just the rest?!
 * TODO Tradeoff: Either let w also include unused features, or extract only the used
 * V1 (big w): No costs for extraction of used features in w. Requires knowledge of total features. Might take longer to multiply (depends on implementation of sparse vector)
 * V2 (small w): Use DenseVector. Fast to multiply (if basemodel is small). If basemodel is big, 
 */
public class IncrementalModelOld {
  
  private Vector w;
  private List<Integer> usedDimensions;
  private List<Integer> unusedDimensions;

  /**
   * Create empty model where all dimensions are unused
   */
  public IncrementalModelOld(double biasValue, int numDimensions) {
    usedDimensions = Lists.newArrayList();
    usedDimensions.add(0);    // Bias
    unusedDimensions = Lists.newArrayListWithCapacity(numDimensions);
    for (int i=1; i<=numDimensions; ++i) { unusedDimensions.add(i); }    // considers bias
    
    w = new DenseVector(1);
    w.set(0, biasValue);
  }
  
  /**
   * Create empty model where all dimensions are unused
   */
  public IncrementalModelOld(double biasValue, List<Integer> remainingDimensions) {
    usedDimensions = Lists.newArrayList();
    usedDimensions.add(0);    // Bias
    this.unusedDimensions = remainingDimensions;
    // TODO: Consider bias. Bad to remap features, isn't it?
    
    w = new DenseVector(1);
    w.set(0, biasValue);
  }
  
  public IncrementalModelOld(List<Integer> usedDimensions, List<Integer> remainingDimensions, Vector w) {
    this.usedDimensions = usedDimensions;
    this.unusedDimensions = remainingDimensions;
    this.w = w;
  }

  /**
   * Return a copy of the base model with place for one more feature
   */
  public Vector getExtendedModel() {
    Vector extended = new DenseVector(w.size()+1);
    for (int i=0; i<w.size(); ++i) {
      extended.setQuick(i, w.getQuick(i));
    }
    return extended; 
  }
  
  public Vector getW() {
    return w;
  }
  
  public void addDimensionToModel(int d, double weight) {
    unusedDimensions.remove(unusedDimensions.indexOf(d));
    usedDimensions.add(d);
    w = getExtendedModel();
    w.setQuick(w.size()-1, weight);
  }
  
  public List<Integer> getUnusedDimensions() {
    return unusedDimensions;
  }
  
  public List<Integer> getUsedDimensions() {
    return usedDimensions;
  }
}