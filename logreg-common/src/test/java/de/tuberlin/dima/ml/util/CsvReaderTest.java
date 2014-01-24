package de.tuberlin.dima.ml.util;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.util.List;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.util.CsvReader;
import de.tuberlin.dima.ml.util.MLUtils;

public class CsvReaderTest {
  
  private static final String TESTFILE = "simple.csv";
  private static final String TARGETNAME = "b2";

  @Test
  public void test() throws Exception {
    
    CsvReader csv;
    // Read data into matrix
    csv = new CsvReader();
    int rows = 4;

    BufferedReader reader = new BufferedReader(MLUtils.open(TESTFILE));
    List<String> predictorNames = Lists.newArrayList(new String[] { "c3", "a1", "d4" });
    csv.numericToDenseMatrix(reader, rows, TARGETNAME, predictorNames, true);
    
    Vector expected = new DenseVector(new double[] {1d, 23d, 21d, 24d});
    System.out.println("Expected: " + expected);
    System.out.println("Is:       " + csv.getData().viewRow(1));
    
    assertTrue(MLUtils.compareVectors(expected, csv.getData().viewRow(1)));
    

    System.out.println("Columns:");
    for (int i=0; i<=csv.getNumPredictors(); ++i) {
      System.out.println(" " + i + ": " + csv.getColumnName(i));    
    }
  }

}
