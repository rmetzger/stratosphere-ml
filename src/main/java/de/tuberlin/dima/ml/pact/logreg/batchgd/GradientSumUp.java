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

import java.util.Iterator;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * Sums up the gradients from subsets of the data to a global gradient. This is
 * possible because the gradient is in our case the sum of the gradients of the
 */
public class GradientSumUp extends ReduceStub {
  
  // Has to be similar because we want to use the reduce method as combiner 
  public static final int IDX_MODEL_KEY = 0;
  public static final int IDX_GRADIENT_PART = 1;
  public static final int IDX_TOTAL = 2;
  public static final int IDX_CORRECT = 3;

  @Override
  public void reduce(Iterator<PactRecord> gradientParts,
      Collector<PactRecord> out) throws Exception {

    // Start with values from first record
    PactRecord first = gradientParts.next();
    PactInteger modelKey = first.getField(IDX_MODEL_KEY, PactInteger.class);
    Vector gradient = first.getField(IDX_GRADIENT_PART, PactVector.class).getValue();
    int total = first.getField(IDX_TOTAL, PactInteger.class).getValue();
    int correct = first.getField(IDX_CORRECT, PactInteger.class).getValue();
    PactRecord record = null;
    while (gradientParts.hasNext()) {
      // Gradient sum up
      record = gradientParts.next();
      Vector gradientPart = record.getField(IDX_GRADIENT_PART, PactVector.class).getValue();
      gradient.assign(gradientPart, Functions.PLUS);

      // In sample validation
      total += record.getField(IDX_TOTAL, PactInteger.class).getValue();
      correct += record.getField(IDX_CORRECT, PactInteger.class).getValue();
    }
    
    PactRecord recordOut = new PactRecord();
    recordOut.setField(ApplyGradient.IDX_INPUT2_MODEL_KEY, modelKey);
    recordOut.setField(ApplyGradient.IDX_INPUT2_GRADIENT, new PactVector(gradient));
    out.collect(recordOut);
    
    // TODO Forward Validation results
    System.out.println("--------\nIN-SAMPLE-VALIDATION\n--------");
    System.out.println("ACCURACY (training-data, last model): " + ((double)correct / (double)total) + " (= " + correct + " / " + total + ")");
    System.out.println("--------");
  }

}
