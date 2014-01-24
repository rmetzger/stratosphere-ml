package de.tuberlin.dima.ml.logreg.sequential;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.mahout.classifier.evaluation.Auc;
import org.apache.mahout.classifier.sgd.L1;
import org.apache.mahout.classifier.sgd.OnlineLogisticRegression;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import com.google.common.base.Stopwatch;

import de.tuberlin.dima.ml.inputreader.RCV1VectorReader;
import de.tuberlin.dima.ml.util.MLUtils;

/**
 * Sequential logistic regression for RCV1-v2
 * Uses Mahout Online Stochastic Gradient Descent (SGD) 
 */
public class LogRegSGDRCV1 {
    
  private static final int FEATURES = 90000;
  private static final int TARGETS = 2;
  
  private Stopwatch stop = new Stopwatch();
    
  public void trainRCV1(String trainingFile, String testFile) throws IOException {
    
    int testLimit = 50000;  // test only first ... lines of test file
    
    // TRAIN
    OnlineLogisticRegression learningAlgorithm = new OnlineLogisticRegression(
    TARGETS, FEATURES, new L1());
    learningAlgorithm.alpha(1).stepOffset(1000)
    .decayExponent(0.2)
    .lambda(3.0e-5)
    .learningRate(20);
    // .alpha(1).stepOffset(1000)
    // decayExponent(0.9)
    // lambda: 3.0e-5
    // learningRate: 20
    
    // READ TARGETS
    stop.start();
    int totalItems = 900000;
    Vector yC = new DenseVector(totalItems);
    Vector yE = new DenseVector(totalItems);
    Vector yG = new DenseVector(totalItems);
    Vector yM = new DenseVector(totalItems);
    RCV1VectorReader.readLabels("/home/andre/dev/datasets/RCV1-v2/rcv1-v2.topics.qrels", yC, yE, yG, yM);
    System.out.println("Reading targets: " + stop.stop().toString());
    Vector y = yC;  // We only train a one-vs-all classifier for a single category
    
    // TODO Bug: Stochastic trainer assumes that data are stored randomly in file

    // TRAINING
    stop.reset().start();
    BufferedReader reader = MLUtils.open(trainingFile);
    int lines = 0;
    int correct = 0;
    String line;
    while ((line = reader.readLine()) != null) {
      
      // Parse & transform to vector
      Vector v = new RandomAccessSparseVector(FEATURES);
      int docId = RCV1VectorReader.readVector(v, line);
            
      // Test prediction
      int actualTarget = (int)y.get(docId);
      double prediction = learningAlgorithm.classifyScalar(v);
      if (actualTarget == Math.round(prediction)) ++correct;
      
      // Train
      learningAlgorithm.train(actualTarget, v);
      
//      if (lines > 1000) break;
//      if (lines % 1000 == 0)
//        System.out.println(actualTarget + " - " + prediction);
      
      ++lines;
    }
    reader.close();
    System.out.println("Lines: " + lines);
    System.out.println("Correct: " + correct);
    System.out.println("Accuracy: " + ((double)correct)/((double)lines));
    learningAlgorithm.close();
    Matrix beta = learningAlgorithm.getBeta();
    System.out.println(beta.viewRow(0).viewPart(0, 100));
    System.out.println("Training: " + stop.stop().toString());
    
    System.out.println("----------------");
    System.out.println("TEST");
    System.out.println("----------------");
    
    stop.reset().start();
    reader = MLUtils.open(testFile);
    lines = 0;
    correct = 0;
    Auc aucGlobal = new Auc();
//    OnlineAuc aucOnline = new GlobalOnlineAuc();    // Difference: Makes auc estimate available always
    while ((line = reader.readLine()) != null) {
      
      Vector v = new RandomAccessSparseVector(FEATURES);
      int docId = RCV1VectorReader.readVector(v, line);
            
      int actualTarget = (int)y.get(docId);
      double prediction = learningAlgorithm.classifyScalar(v);
      if (actualTarget == Math.round(prediction)) ++correct;
      
      aucGlobal.add(actualTarget, prediction);
//      aucOnline.addSample(actualTarget, prediction);
      
      if (++lines >= testLimit) break;
    }
    reader.close();
    System.out.println("Testing: " + stop.stop().toString());
    System.out.println("Lines: " + lines);
    System.out.println("Correct: " + correct);
    System.out.println("Accuracy: " + ((double)correct)/((double)lines));
    System.out.println("Global AUC: " + aucGlobal.auc());
//    System.out.println("Online AUC: " + aucOnline.auc());
//    System.out.println(aucGlobal.confusion().asFormatString());
  }
  
  public static void main(String[] args) throws IOException {
    String trainingFile = "/home/andre/dev/datasets/RCV1-v2/vectors/lyrl2004_vectors_train_5000.dat";
    String testFile = "/home/andre/dev/datasets/RCV1-v2/vectors/lyrl2004_vectors_test_pt0.dat";
    
    LogRegSGDRCV1 lr = new LogRegSGDRCV1();
    lr.trainRCV1(trainingFile, testFile);
  }
  
}
