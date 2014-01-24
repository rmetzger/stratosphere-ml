package de.tuberlin.dima.ml.logreg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.time.StopWatch;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.visualization.datasource.base.TypeMismatchException;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.render.JsonRenderer;

import de.tuberlin.dima.ml.logreg.LogRegDiffFunction;
import de.tuberlin.dima.ml.logreg.LogRegModel;
import de.tuberlin.dima.ml.logreg.LogRegTraining;
import de.tuberlin.dima.ml.util.CsvReader;
import de.tuberlin.dima.ml.util.MLUtils;
import de.tuberlin.dima.ml.validation.Validation;
import edu.stanford.nlp.optimization.QNMinimizer;

public class LogisticRegressionTest {
  
  public static List<String> predictorNames;
  
  public static final String testFile = "donut-test.csv";
  public static final String trainingFile = "donut.csv";
  private static final double TARGET_POSITIVE = 2d;
  private static final double TARGET_NEGATIVE = 1d;
  private static final String TARGET_NAME = "color";
  
  private CsvReader csvTest;
  private CsvReader csvTrain;
  
  @Before
  public void before() throws Exception {
    
    // See SFOSequentialTest for info
    predictorNames = Lists.newArrayList(new String[] {
      "x", "y", "shape", "xx", "xy", "yy", "a", "b", "c"    // nice chart
//      "x", "y", "shape", "a", "b", "c"
//      "x", "y", "a", "b", "c"    // like Mahout page 252
//      "x", "c"    // Adding a or b is REALLY BAD. However x and y are good. Shape is okay.
     });
    
    csvTrain = MLUtils.readData(trainingFile, 40, predictorNames, TARGET_NAME, true);
    csvTrain.normalize();
    csvTrain.normalizeClassLabels(TARGET_POSITIVE, TARGET_NEGATIVE);
    csvTest = MLUtils.readData(testFile, 40, predictorNames, TARGET_NAME, true);
    csvTest.normalize(csvTrain.getMeans(), csvTrain.getRanges());
//    csvTest.normalize();
    csvTest.normalizeClassLabels(TARGET_POSITIVE, TARGET_NEGATIVE);
  }
  
  @Test
  @Ignore
  public void testTrainNewton() {

    System.out.println("-----------------");
    System.out.println("Train Newton");
    System.out.println("-----------------");
    
     double initialWeight = 0;
//     LogisticRegression logReg = new LogisticRegression("donut.csv", predictorNames);

//       StopWatch sw = new StopWatch();
//       sw.start();
//       sw.stop();

     int iterations = 20;
     List<Double> penalties = Lists.newArrayList(new Double[] {0d, 0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1d, 2d, 5d, 10d, 20d, 50d, 100d, 500d, 1000d});
     
     DataTable data = new DataTable();
     ArrayList<ColumnDescription> cd = new ArrayList<ColumnDescription>();
     cd.add(new ColumnDescription("lambda", ValueType.NUMBER, "Regularization constraint"));
     cd.add(new ColumnDescription("Success_in", ValueType.NUMBER, "in-sample success"));
     cd.add(new ColumnDescription("Success_out", ValueType.NUMBER, "out-of-sample success"));
     data.addColumns(cd);
     for (double lambda : penalties) {
       Vector w = LogRegTraining.trainNewton(csvTrain.getData(), csvTrain.getY(), iterations, initialWeight, lambda);   // breaks at 92 without regularization
//         logReg.printLinearModel(w);
       Validation valTest = new Validation();
       Validation valTrain = new Validation();
       LogRegModel logReg = new LogRegModel(w);
       valTest.computeMetrics(csvTest.getData(), csvTest.getY(), logReg, logReg);
       valTrain.computeMetrics(csvTrain.getData(), csvTrain.getY(), logReg, logReg);
       System.out.println("Regularization: " + lambda);
       System.out.println("- Accuracy:\t\t" + valTest.getAccuracy());
       System.out.println("- Mean Deviation:\t" + valTest.getMeanDeviation());

       try {
         data.addRowFromValues(lambda, valTrain.getAccuracy(), valTest.getAccuracy());
       } catch (TypeMismatchException e) {
         System.out.println("Invalid type!");
       }
     }
//       System.out.println("CONFUSION");
//       System.out.println("True Pos:\t" + (int)valTest.getConfusion().get(0, 0));
//       System.out.println("True Neg:\t" + (int)valTest.getConfusion().get(1, 1));
//       System.out.println("False Pos:\t" + (int)valTest.getConfusion().get(0, 1));
//       System.out.println("False Neg:\t" + (int)valTest.getConfusion().get(1, 0));
     
     String json = "result='" + JsonRenderer.renderDataTable(data, true, true, false).toString() + "'";
     MLUtils.writeUtf(json, "results.json");
  }

  @Ignore
  @Test
  public void testBatchGD() {

    System.out.println("\n-----------------");
    System.out.println("Train Batch GD");
    System.out.println("-----------------");
    
    double initialWeight = 0;
    StopWatch sw = new StopWatch();
    sw.start();
    int iterations = 50;
    Vector w = LogRegTraining.trainBatchGD(csvTrain.getData(), csvTrain.getY(), iterations, 1, initialWeight);

    Validation valTest = new Validation();
    Validation valTrain = new Validation();
    LogRegModel logRegValidation = new LogRegModel(w);
    valTest.computeMetrics(csvTest.getData(), csvTest.getY(), logRegValidation, logRegValidation);
    valTrain.computeMetrics(csvTrain.getData(), csvTrain.getY(), logRegValidation, logRegValidation);
    
    MLUtils.printLinearModel(w, csvTrain);
    System.out.println("Evaluation");
    System.out.println("- Accuracy:\t\t" + valTest.getAccuracy());
    System.out.println("- Mean Deviation:\t" + valTest.getMeanDeviation());
  }
  
    @Test
    // Test Logistic Regression with L-BFGS minimizer from edu.stanford.nlp.optimization.QNMinimizer
    public void testLBFGS() {
      
      System.out.println("\n-----------------");
      System.out.println("Train L-BFGS:");
      System.out.println("-----------------");
    
      // Input
      double[] initial = new double[this.csvTrain.getData().numCols()];
      Arrays.fill(initial, 1);
      
      Matrix input = this.csvTrain.getData();
      Vector labels = this.csvTrain.getY();
      LogRegDiffFunction f = new LogRegDiffFunction(input, labels);
      
      // Different arguments (e.g. 0.001 and 21) yield comparable results (accuracy and mean dev)  
      double eps = 0.1;
      int maxIterations = 13;
      
      //  Training
      QNMinimizer qn = new QNMinimizer(15, true);
      Vector model = new DenseVector(qn.minimize(f, eps, initial, maxIterations));
      
      // Validation
      Validation validationTest = new Validation();
      Validation validationTraining = new Validation();
      LogRegModel logRegValidation = new LogRegModel(model);
      
      validationTest.computeMetrics(this.csvTest.getData(), this.csvTest.getY(),
              logRegValidation, logRegValidation);
      validationTraining.computeMetrics(this.csvTest.getData(), this.csvTrain.getY(),
              logRegValidation, logRegValidation);
      
      MLUtils.printLinearModel(model, this.csvTrain);
      System.out.println("Evaluation:");
      System.out.println("- Accuracy: " + validationTest.getAccuracy());
      System.out.println("- Mean Deviation: " + validationTest.getMeanDeviation());
      System.out.println("Statistics");
      System.out.println("- DeriveAt counter: " + f.getCountDeriveAt());
      System.out.println("- ValueAt counter: " + f.getCountValueAt());
  }
}