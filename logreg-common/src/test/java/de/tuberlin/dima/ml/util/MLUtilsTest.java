package de.tuberlin.dima.ml.util;
import static org.junit.Assert.assertEquals;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import de.tuberlin.dima.ml.util.MLUtils;


public class MLUtilsTest {

  @Test
  public void test() {
    Matrix data = new DenseMatrix(3,2);
    data.set(0, new double[] {1, -3});
    data.set(1, new double[] {2, -1});
    data.set(2, new double[] {3, 1});
    
    Vector mean = MLUtils.meanByColumns(data);
    assertEquals(mean.get(0), 2, 0);
    assertEquals(mean.get(1), -1, 0);
    
    Vector range = MLUtils.rangeByColumns(data);
    assertEquals(range.get(0), 2, 0);
    assertEquals(range.get(1), 4, 0);
    
    Matrix m = new DenseMatrix(2,2);
    m.set(0, new double[] {2, 0});
    m.set(1, new double[] {0, -4});
    
    Matrix inverse = MLUtils.inverse(m);
    Matrix res = inverse.times(m);
    assertEquals(res.get(0, 0), 1, 0);
    assertEquals(res.get(1, 1), 1, 0);
    assertEquals(res.get(0, 1), 0, 0);
    assertEquals(res.get(1, 0), 0, 0);
  }

}
