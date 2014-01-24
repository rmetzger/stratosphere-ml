package de.tuberlin.dima.ml.linreg;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Test;


import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.linreg.LinearRegression;

public class LinearRegressionTest {

  @Test
  public void test() throws Exception {
    List<String> predictorNames = Lists.newArrayList(new String[] {
//      "x", "y", "shape", "k", "k0", "xx", "xy", "yy", "a", "b", "c", "bias"
//      "x", "y", "shape", "a", "b", "c"
//      "x", "y", "a", "b", "c"    // like Mahout page 252
      "x", "c"    // like Mahout page 252
     });

    LinearRegression trainer = new LinearRegression();
    try {
      trainer.train("donut.csv", predictorNames);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
  }

}
