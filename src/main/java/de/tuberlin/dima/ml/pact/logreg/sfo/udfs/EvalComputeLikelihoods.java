package de.tuberlin.dima.ml.pact.logreg.sfo.udfs;

import org.apache.mahout.math.Vector;

import de.tuberlin.dima.ml.logreg.LogRegMath;
import de.tuberlin.dima.ml.logreg.sfo.IncrementalModel;
import de.tuberlin.dima.ml.logreg.sfo.SFOGlobalSettings;
import de.tuberlin.dima.ml.pact.logreg.sfo.PactIncrementalModel;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * This UDF processes one input vector (x_i, y_i) at a time, where x_i is
 * assumed to be sparse. For every non-zero value in x_i, it computes the gain
 * in log-likelihood when adding the trained coefficient for this feature.
 * 
 * Some remarks on how we (efficiently) compute the gain: The number of features
 * and input records is assumed to be large, so it is infeasible to compute the
 * complete metric (e.g. log-likelihood) for all models. So we only compute the
 * gain regarding log-likelihood compared to the base model. To compute the gain
 * for a model, which differs from base model just in on additional coefficient,
 * we only need to compute the likelihood for the items that actually have this
 * feature. In sparse models (text), this safes a lot of time.
 */
public class EvalComputeLikelihoods extends CrossStub {
  
  public static final int IDX_INPUT1_INPUT_RECORD = 0;
  public static final int IDX_INPUT1_LABEL = 1;
  
  public static final int IDX_INPUT2_BASEMODEL = 0;
  public static final int IDX_INPUT2_TRAINED_COEFFICIENTS = 1;
  
  public static final int IDX_OUT_DIMENSION = EvalSumLikelihoods.IDX_DIMENSION;
  public static final int IDX_OUT_LL_BASE = EvalSumLikelihoods.IDX_LL_BASE;
  public static final int IDX_OUT_LL_NEW = EvalSumLikelihoods.IDX_LL_NEW;
  
  private boolean baseModelAndCoefficientsCached = false;
  private IncrementalModel baseModel = null;
  Vector coefficients = null;

  private final PactRecord recordOut = new PactRecord(3);
  
  @Override
  public void open(Configuration parameters) throws Exception {
	// Dangerous: When using iterations, the udf instance will be reused
	// and we have to make sure to deserialize again.
	baseModelAndCoefficientsCached = false;
  }

  // The system has to create a new copy of baseModelAndCoefficients for every call to guaranty that it is the same for every call 
  // If the system would pass a reference the udf could modify it
  @Override
  public void cross(PactRecord testRecord, PactRecord baseModelAndCoefficients,
      Collector<PactRecord> out) throws Exception {

    int yi = testRecord.getField(IDX_INPUT1_INPUT_RECORD, PactInteger.class).getValue();
    Vector xi = testRecord.getField(IDX_INPUT1_LABEL, PactVector.class).getValue();
    
    // Manual optimization: Cache base model and trained coefficients, will always be the same
    if (!baseModelAndCoefficientsCached) {
      baseModel = baseModelAndCoefficients.getField(IDX_INPUT2_BASEMODEL, PactIncrementalModel.class).getValue();
      coefficients = baseModelAndCoefficients.getField(IDX_INPUT2_TRAINED_COEFFICIENTS, PactVector.class).getValue();
      baseModelAndCoefficientsCached = true;
    }

    // Compute log-likelihood for current x_i using the base model (without new coefficient)
    double piBase = LogRegMath.predict(xi, baseModel.getW(), SFOGlobalSettings.INTERCEPT);
    double llBase = LogRegMath.logLikelihood(yi, piBase);

    for (Vector.Element feature : xi.nonZeroes()) {
      int dim = feature.index();
      if (! baseModel.isFeatureUsed(dim)) {
    	double coefficient = coefficients.get(dim);
    	// Features with coefficient 0 were either not in our training data
    	//  or were considered to be not important. We don't need to evaluate these features
    	if (coefficient != 0) {
    	  // Extend the base model by the current coefficient, revert afterwards
          baseModel.getW().set(dim, coefficient);
          double piNew = LogRegMath.logisticFunction(xi.dot(baseModel.getW()) + SFOGlobalSettings.INTERCEPT);
          baseModel.getW().set(dim, 0d);

          double llNew = LogRegMath.logLikelihood(yi, piNew);
          
          recordOut.setField(IDX_OUT_DIMENSION, new PactInteger(dim));
          recordOut.setField(IDX_OUT_LL_BASE, new PactDouble(llNew - llBase));
          out.collect(recordOut);
    	}
      }
    }
  }

}
