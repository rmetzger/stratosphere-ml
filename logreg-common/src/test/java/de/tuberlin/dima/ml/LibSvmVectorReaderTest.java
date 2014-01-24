package de.tuberlin.dima.ml;

import static org.junit.Assert.assertEquals;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import de.tuberlin.dima.ml.inputreader.LibSvmVectorReader;

public class LibSvmVectorReaderTest {

  @Test
  public void testReadVectorMultiLabel() {
    int labelComputed = 0;
    
    int labelDimension = 2;
    
    // This is a vector not belonging to our class (negative)
    String line1 = "1  10:1.12345 20:-2.12345";
    Vector v1 = new RandomAccessSparseVector(21);
    labelComputed = LibSvmVectorReader.readVectorMultiLabel(v1, line1, labelDimension);
    assertEquals(0, labelComputed);
    assertEquals(v1.getNumNonZeroElements(), 2);
    assertEquals(v1.get(10), 1.12345, 0);
    assertEquals(v1.get(20), -2.12345, 0);
    
    // This is a vector belonging to our class (positive)
    String line2 = "2  10:1.12345 20:-2.12345";
    Vector v2 = new RandomAccessSparseVector(21);
    labelComputed = LibSvmVectorReader.readVectorMultiLabel(v2, line2, labelDimension);
    assertEquals(1, labelComputed);
    assertEquals(v2.getNumNonZeroElements(), 2);
    assertEquals(v2.get(10), 1.12345, 0);
    assertEquals(v2.get(20), -2.12345, 0);
    
    // This is a vector belonging to our class (positive)
    String lineMultiLabel = "4,5,2  10:1.12345 20:-2.12345";
    Vector v3 = new RandomAccessSparseVector(21);
    labelComputed = LibSvmVectorReader.readVectorMultiLabel(v3, lineMultiLabel, labelDimension);
    assertEquals(1, labelComputed);
    assertEquals(v3.getNumNonZeroElements(), 2);
    assertEquals(v3.get(10), 1.12345, 0);
    assertEquals(v3.get(20), -2.12345, 0);
  }
  
  @Test
  public void testReadVectorSingleLabel() {
    int labelComputed;
    Vector v;
    String positive = "+1 10:1.12345 20:-2.12345";
    v = new RandomAccessSparseVector(21);
    labelComputed = LibSvmVectorReader.readVectorSingleLabel(v, positive);
    assertEquals(1, labelComputed);
    assertEquals(v.getNumNonZeroElements(), 2);
    assertEquals(v.get(10), 1.12345, 0);
    assertEquals(v.get(20), -2.12345, 0);
    
    String negative = "-1 10:1.12345 20:-2.12345";
    v = new RandomAccessSparseVector(21);
    labelComputed = LibSvmVectorReader.readVectorSingleLabel(v, negative);
    assertEquals(0, labelComputed);
    assertEquals(v.getNumNonZeroElements(), 2);
    assertEquals(v.get(10), 1.12345, 0);
    assertEquals(v.get(20), -2.12345, 0);
  }

}
