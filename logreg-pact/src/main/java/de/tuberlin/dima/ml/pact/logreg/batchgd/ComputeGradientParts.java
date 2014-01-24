/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package de.tuberlin.dima.ml.pact.logreg.batchgd;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.types.IntValue;
import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.logreg.LogRegMath;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.util.Collector;
import eu.stratosphere.types.Record;


/**
 * Computes the gradient for the logistic regression function for a single data
 * point and a given model (weight vector).
 */
public class ComputeGradientParts extends CrossFunction {
  
  public static final int IDX_INPUT1_INPUT_RECORD = 0;
  public static final int IDX_INPUT1_LABEL = 1;
  
  public static final int IDX_INPUT2_MODEL = 0;
  
  private final Record recordOut = new Record();
  private final PactVector vectorOut = new PactVector();
  private static final IntValue zero = new IntValue(0);
  private static final IntValue one = new IntValue(1);
  
  private static int totalProcessed = 0;

  /**
   * TODO Feature: Produce multiple models, e.g. with different regularization
   */
	@Override
	public void cross(Record trainingVector, Record model, Collector<Record> out) throws Exception {

        int y = trainingVector.getField(IDX_INPUT1_INPUT_RECORD, IntValue.class).getValue();
		Vector x = trainingVector.getField(IDX_INPUT1_LABEL, PactVector.class).getValue();
		Vector w = model.getField(IDX_INPUT2_MODEL, PactVector.class).getValue();
		
//		System.out.println("Training vector: size=" + xTrain.size() + " non-zeros=" + xTrain.getNumNonZeroElements());

        Vector gradient = LogRegMath.computePartialGradient(x, w, y);

        // In sample validation
        double prediction = LogRegMath.classify(x, w);
        
        vectorOut.setValue(gradient);
        recordOut.setField(GradientSumUp.IDX_MODEL_KEY, one);
        recordOut.setField(GradientSumUp.IDX_GRADIENT_PART, vectorOut);
        recordOut.setField(GradientSumUp.IDX_TOTAL, one);
        recordOut.setField(GradientSumUp.IDX_CORRECT, (prediction == y)?one:zero);
        out.collect(recordOut);
        
        totalProcessed++;
        if (totalProcessed%1000 == 0)
          System.out.println("totalProcessed=" + totalProcessed);
	}

}
