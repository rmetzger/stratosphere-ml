package de.tuberlin.dima.ml.datasets;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Provides information about Donut dataset, as used in Mahout in action.
 * Dataset can be used for test cases
 */
public class DonutDatasetInfo {

  private static final int TOTAL = 40;
  
  private static List<String> predictorNames = Lists.newArrayList(new String[] {
      "x", "y", "shape", "xx", "xy", "yy", "a", "b", "c"
   });
  
  private static final Map<Integer, String> labelMap = ImmutableMap.of(
      0, "color");

  private static DatasetInfo datasetInfo = new DatasetInfo.Builder()
      .numFeatures(predictorNames.size())
      .total(TOTAL)
      .labelMap(labelMap)
      .predictorNames(predictorNames).build();
  
  public static DatasetInfo get() {
    return datasetInfo;
  }
  
}
