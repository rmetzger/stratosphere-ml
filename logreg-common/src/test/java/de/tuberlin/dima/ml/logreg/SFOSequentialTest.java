package de.tuberlin.dima.ml.logreg;


import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.logreg.sfo.SFOSequential;

public class SFOSequentialTest {
  public static List<String> predictorNames;
  
  public static final String TESTFILE = "donut-test.csv";
  public static final String TRAININGFILE = "donut.csv";
  
  @Before
  public void before() throws IOException {
    /**
     * Usable features: "x", "y", "shape", "xx", "xy", "yy", "a", "b", "c"
     * 
     * Unusable features
     * - color is the label
     * - k, k0 are only in training file, not in test file!
     * - bias is static value (1)
     * 
     * Experiences
     * - Best feature: c
     * - Adding a or b (to c) is REALLY BAD. However x and y are good
     * - like Mahout page 252: "x", "y", "a", "b", "c"
     */
    predictorNames = Lists.newArrayList(new String[] {
      "x", "y", "shape", "xx", "xy", "yy", "a", "b", "c"    // nice chart
     });
  }
  
  @Test
  public void test() throws Exception {
    try {
      SFOSequential sfo = new SFOSequential(TRAININGFILE, TESTFILE, predictorNames);
      
      int iterations = 1;
      for (int i=1; i<=iterations; ++i) {
        sfo.findBestFeature();
      }
      /*
       * First row training: "x","y","shape","color","k","k0","xx","xy","yy","a","b","c","bias"
       * First row test: "x","y","shape","color","xx","xy","yy","c","a","b"
       * 
       * TRAIN not transformed
       * x       0.802415437065065
       * y       0.0978854028508067
       * shape   21
       * xx      2
       * xy      0.643870533640319
       * yy      0.07854475831082
       * c       0.00958155209126472
       * a       0.503141377562721
       * b       0.808363832523192
       * c       0.220502180491382
       * 
       * TRAIN Transformed
       * 0:      1.0
       * x:      0.43745573940034516
       * y:     -0.5186781189990234
       * shape: -0.5
       * xx:     0.5147761286706689
       * xy:    -0.30266081924966975
       * yy:    -0.37394758729433375
       * a:      0.11113746575013662
       * b:     -0.5647616676889982
       * c:      0.432058745119435
       * 
       * TEST Transformed
       * 0:      1.0
       * x:      0.2439223917274277
       * y:     -0.4621642214895388
       * shape: -0.5562500000000004
       * xx:     0.24751382095920688
       * xy:    -0.25621102757158987
       * yy:    -0.3764021162352499
       * c:      0.20583668348697667
       * a:     -0.014914454196422918
       * b:     -0.6047124875052861
       */
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
