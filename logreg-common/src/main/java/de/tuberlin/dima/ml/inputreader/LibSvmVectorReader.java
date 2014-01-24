package de.tuberlin.dima.ml.inputreader;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.mahout.math.Vector;

import com.google.common.base.Splitter;

public class LibSvmVectorReader {
  
  private static final Splitter LIBSVM_SPLITTER = Splitter.on(Pattern.compile("[ :]"))
      .trimResults()
      .omitEmptyStrings();
  
  private static final Splitter LABEL_SPLITTER = Splitter.on(Pattern.compile(","));

  private LibSvmVectorReader() {};
  
  
  /**
   * Reads a single line in libsvm format (multilabel or single label)
   * Line format:   label1,label2,...,labeln  feature-id-1:val-1 feature-id-2:val-2 ...
   * 
   * @return 

   * @param v
   * @param line
   * @param labelIndex 
   * @return Label of the data, either 0 (negative class) or 1 (positive class) 
   */
  public static int readVectorMultiLabel(Vector v, String line, int labelIndex) {
    
    Iterator<String> iter = LIBSVM_SPLITTER.split(line).iterator();
    
    // Process labels
    String allLabels = iter.next();
    boolean isPositive = false;
    int label;
    Iterator<String> labelIter = LABEL_SPLITTER.split(allLabels).iterator();
    while (labelIter.hasNext()) {
      label = Integer.parseInt(labelIter.next());
      if (label == labelIndex) {
        isPositive = true;
      }
    }
    
    // Process features (sparse)
    int featureId;
    Double featureVal;
    while (iter.hasNext()) {
      featureId = Integer.parseInt(iter.next());
      featureVal = Double.parseDouble(iter.next());
      v.set(featureId, featureVal);
    }
    return (isPositive) ? 1 : 0;
  }
  

  public static int readVectorSingleLabel(Vector v, String line) {
    
    Iterator<String> iter = LIBSVM_SPLITTER.split(line).iterator();
    
    // Get label. Might be in format "+1"
    String labelString = iter.next().replace("+", "");
    int label = Integer.parseInt(labelString);
    
    // Process features (sparse)
    int featureId;
    Double featureVal;
    while (iter.hasNext()) {
      featureId = Integer.parseInt(iter.next());
      featureVal = Double.parseDouble(iter.next());
      v.set(featureId, featureVal);
    }
    return (label == 1) ? 1 : 0;
  }

}
