package de.tuberlin.dima.ml.pact;

import eu.stratosphere.example.java.record.kmeans.KMeansIterative;
import eu.stratosphere.example.java.record.kmeans.KMeansSampleDataGenerator;

public class TestKMeans {
  
  public static void main(String[] args) throws Exception {
    // GENERATE TEST DATA
    String numPoints = "1000";
    String numClusters = "5";
    KMeansSampleDataGenerator.main(new String[] { numPoints, numClusters });

    // RUN JOB
    String dataDir = "file:///Users/qml_moon/Documents/TUB/DIMA/code/stratosphere-ml/logreg-pact/";
    String output = "file:///Users/qml_moon/Documents/TUB/DIMA/code/output-kmeans";
    String numSubTasks = "2";
    String numIterations = "2";
    JobRunner runner = new JobRunner();
    // <numSubStasks> <dataPoints> <clusterCenters> <output> <numIterations>
    String[] jobArgs = new String[] { 
        numSubTasks,
        dataDir + "points", 
        dataDir + "centers", 
        output, 
        numIterations
    };
    runner.runLocal(new KMeansIterative().getPlan(jobArgs));
  }

}
