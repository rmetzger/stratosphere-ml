package de.tuberlin.dima.ml.datasets;

import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

/**
 * Provides additional information about a dataset (usually meta data)
 * E.g. mapping from feature dimension to feature name, number of features or size
 * 
 * For MapReduce we can make static instances of this class,
 *  that are accessible also from Mappers and Reducers
 */
public class DatasetInfo {

  private long numFeatures=0;
  private long total=0;
  private List<String> featureNames = Lists.newArrayList();
  private BiMap<Integer, String> labelMap = HashBiMap.create();
  
  /**
   * Builder pattern (effective java, item 2)
   * Makes object immutable and easy to add optional parameters
   */
  public static class Builder {
    
    // Required parameters (currently none)
    
    // Optional parameters
    private long numFeatures = 0;
    private long total = 0;
    private List<String> featureNames = null;
    private BiMap<Integer, String> labelMap = null;
    
    public Builder() {
    }
    
    public Builder numFeatures(long val) {
      numFeatures = val; return this;
    }
    
    public Builder total(long val) {
      total = val; return this;
    }
    
    public Builder predictorNames(List<String> val) {
      featureNames = val; return this;
    }
    
    public Builder labelMap(Map<Integer, String> val) {
      labelMap = HashBiMap.create(val); return this;
    }
    
    public DatasetInfo build() {
      return new DatasetInfo(this);
    }
  }
  
  private DatasetInfo(Builder builder) {
    numFeatures = builder.numFeatures;
    total = builder.total;
    featureNames = builder.featureNames;
    labelMap = builder.labelMap;
  }
  
  public String getFeatureName(int dimension) {
    if (featureNames != null && featureNames.size() > dimension)
      return featureNames.get(dimension);
    return "unknown feature";
  }
  
  public long getTotal() {
    return total;
  }
  
  public String getLabelNameById(int id) {
    return labelMap.get(id);
  }
  
  public int getLabelIdByName(String name) {
    return labelMap.inverse().get(name);
  }
  
  public long getNumFeatures() {
    return numFeatures;
  }
  
  public void setPredictorNames(List<String> predictorNames) {
    this.featureNames = predictorNames;
  }
  
}
