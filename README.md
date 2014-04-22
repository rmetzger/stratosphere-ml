A collection of code for a Stratosphere Machine Learning Library
======

This repository contains a collection of code to create a machine learning library.
Please note that the code here is copied together from multiple sources. 

It is work in progress and does not represent an official implementation of the Stratosphere team.

## Algorithms

| Algorithm     | Status        | Author(s)  |
| ------------- |---------------| --------|
| Streaming K-Means | untested code | @jllopezpino and others |
| K-Means | in examples of Stratosphere | Stratosphere Developers |
| Logistic regression with SGD | untested code | Andre Hacker (@ahacker), Mingliang Qi (@qmlmoon) |
| Logistic regression (new Java API) | not finished | @zentol, see mailing list |
| Item similarity for recommendations  | untested code | Janani Chakkaradhari (@JananiC), Silvia Julinda (@sjulinda), Suryamita Harindrari (@sharindrari) |
| Random forests | untested code | - |
| Matrix factorization | untested code available but not committed | - |


## Logistic Regression

Parallel implementations of Logistic Regression.

How to run logistic regression on stratosphere

1) Ensemble + SGD (mahout) Training

Parameters: [numPartitions] [inputPathTrain] [inputPathTest] [outputPath] [numFeatures] [runValidation (0 or 1)]

example:
bin/stratosphere run -j logreg-pact-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c de.tuberlin.dima.ml.pact.logreg.ensemble.EnsembleJob -a 1 file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr-out 3 0

2) Iterative Batch Gradient descent training

Parameters: [numSubTasks] [inputPathTrain] [inputPathTest] [outputPath] [numIteration] [runValidation (0 or 1)] [learningRate] [positiveClass] [numFeatuer]

example:
bin/stratosphere run -j logreg-pact-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c de.tuberlin.dima.ml.pact.logreg.batchgd.BatchGDPlanAssembler -a 1 file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr-out 10 0 0.05 1 3

3) Forward Feature Selection using SFO (Single Feature optimization)

Parameters: [numSubStasks] [inputPathTrain] [inputPathTest] [isMultiLabel (true/false)] [positiveClass] [outputPath] [numFeatures] [newton tolerance] [newton max iterations] [regularization] [iterations] [addFeaturePerIteration] [Optional: baseModel (base64 encoded)]

example:
bin/stratosphere run -j logreg-pact-0.0.1-SNAPSHOT-jar-with-dependencies.jar -c de.tuberlin.dima.ml.pact.logreg.sfo.SFOPlanAssembler -a 1 file:///Users/qml_moon/Documents/TUB/DIMA/code/lr file:///Users/qml_moon/Documents/TUB/DIMA/code/lr false 1 file:///Users/qml_moon/Documents/TUB/DIMA/code/lr-out 3 0.000001 5 0 2 1 




Developed for DIMA group at TU Berlin www.dima.tu-berlin.de
