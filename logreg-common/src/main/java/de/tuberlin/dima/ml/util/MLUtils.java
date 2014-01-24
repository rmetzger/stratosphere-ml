package de.tuberlin.dima.ml.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SingularValueDecomposition;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.function.VectorFunction;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class MLUtils {
  
  /**
   * Try to open file for reading from Resources.
   * If there is no such resource, try to read from filesystem. 
   * 
   * @param inputFile
   * @return
   * @throws IOException
   */
  public static BufferedReader open(String inputFile) throws IOException {
    InputStream in;
    try {
      in = Resources.getResource(inputFile).openStream();
    } catch (IllegalArgumentException e) {
      in = new FileInputStream(new File(inputFile));
    }
    return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }
  
  public static void writeUtf(String text, String file) {
    Writer out;
    try {
      out = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(file), "UTF-8"));
      out.write(text);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public static boolean compareVectors(Vector first, Vector second) {
    if (first.size() != second.size()) {
//      System.out.println("Vectors have different cardinality");
      return false; 
    }
    for (int i=0; i<first.size(); ++i) {
      if (first.get(i) != second.get(i)) {
//        System.out.println("Vectors are not equal: " + first.get(i) + " vs. " + second.get(i));
        return false;
      }
    }
    return true;
  }
  
  public static Vector meanByColumns(Matrix m) {
    Vector sums = m.aggregateColumns(new VectorFunction() {
      public double apply(Vector col) {
        return col.aggregate(Functions.PLUS, Functions.IDENTITY);
      }
    });
    return sums.divide(m.numRows());
  }
  
  public static Vector rangeByColumns(Matrix m) {
    Vector min = m.aggregateColumns(new VectorFunction() {
      public double apply(Vector f) {
        return f.minValue();
      }
    });
    Vector max = m.aggregateColumns(new VectorFunction() {
      public double apply(Vector f) {
        return f.maxValue();
      }
    });
    return max.minus(min);
  }
  
  public static Vector ones(int d) {
    Vector v = new DenseVector(d);
    return v.assign(1);
  }
  
  public static Matrix diag(Vector diag) {
    Matrix m = new DenseMatrix(diag.size(), diag.size());
    for (int i=0; i<diag.size(); ++i) {
      m.set(i, i, diag.get(i));
    }
    return m;
  }
  
  public static Matrix vectorToColumnMatrix(Vector vec) {
    Matrix m = new DenseMatrix(vec.size(), 1);
    return m.assignColumn(0, vec);
  }
  
  public static Matrix vectorToRowMatrix(Vector vec) {
    Matrix m = new DenseMatrix(1, vec.size());
    return m.assignRow(0, vec);
  }
  
  static void printDimensions(Matrix matrix) {
    System.out.println("Size: " + matrix.rowSize() + "x" + matrix.columnSize());
  }
  
  /**
   * Computes pseudo inverse using SSVD (stochastical singular value decomposition)
   * See http://en.wikipedia.org/wiki/Singular_value_decomposition#Applications_of_the_SVD
   * 
   * @param data Input Matrix
   * @return pseudo-inverse of data matrix
   */
  public static Matrix pseudoInversebySVD(Matrix data) {
    
    SingularValueDecomposition svd = new SingularValueDecomposition(data);
    
    // Use SVD result to compute pseudoinverse of matrix
    // Computation: pinv(data) = V pinv(S) U*
    // where data = U S V* (this is the result of the svd)
    // and   pinv(S) = replacing every non-zero diagonal entry in S by its reciprocal and transposing the resulting matrix
    
    Matrix S = svd.getS();
    
    for (int i=0; i<S.rowSize(); ++i) {
      if (S.get(i, i) != 0) {
        S.set(i, i, 1.0 / S.get(i, i));
      }
    }
    
    return svd.getV().times(S.transpose()).times(svd.getU().transpose());
  }

//  public static CsvReader readDataNormalized(String sampleFile, int rows, List<String> predictorNames, String targetName, double targetPositive, double targetNegative) throws Exception {
//    CsvReader csv;
//    // Read data into matrix
//    BufferedReader reader = new BufferedReader(MLUtils.open(sampleFile));
//    csv = new CsvReader();
//    csv.numericToDenseMatrix(reader, rows, targetName, predictorNames, true);
//    
//    csv.normalize();
//    csv.normalizeClassLabels(targetPositive, targetNegative);
//    
//    return csv;
//  }

  public static CsvReader readData(String sampleFile, int rows, List<String> predictorNames, String targetName, boolean addBias) throws Exception {
    CsvReader csv;
    // Read data into matrix
    BufferedReader reader = new BufferedReader(MLUtils.open(sampleFile));
    csv = new CsvReader();
    csv.numericToDenseMatrix(reader, rows, targetName, predictorNames, addBias);
    
    return csv;
  }
  
  public static void printLinearModel(Vector w, CsvReader csv) {
    System.out.println("Model");
    for (int i=0; i<w.size(); ++i) {
      System.out.println(" - " + csv.getColumnName(i) + "\t" + w.get(i));
    }
  }
  
  public static Matrix inverse(Matrix data) {
    
    SingularValueDecomposition svd = new SingularValueDecomposition(data);
    
    // Use SVD result to compute pseudoinverse of matrix
    // Computation: pinv(data) = V pinv(S) U*
    // where data = U S V* (this is the result of the svd)
    // and   pinv(S) = replacing every non-zero diagonal entry in S by its reciprocal and transposing the resulting matrix
    
    Matrix S = svd.getS();
    
    for (int i=0; i<S.rowSize(); ++i) {
      if (S.get(i, i) != 0) {
        S.set(i, i, 1.0 / S.get(i, i));
      }
    }
    
    return svd.getV().times(S).times(svd.getU().transpose());
  }

}
