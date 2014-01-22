package de.tuberlin.dima.ml.pact.logreg.sfo.udfs;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.logreg.sfo.NewtonSingleFeatureOptimizer;
import de.tuberlin.dima.ml.pact.udfs.ReduceFlattenToVector;
import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;

/**
 * This UDF trains a single dimension at a time.
 * 
 * Let's assume we train dimension d: Then the input consists of N
 * pre-aggregated PACT records, where N is the number of vectors of the original
 * input that have a non-zero value for dimension d. See Singh et al. for details.
 * 
 * @author André Hacker
 */
@ConstantFields({0})
public class TrainDimensions extends ReduceStub {
  
  public static final int IDX_DIMENSION = 0;
  public static final int IDX_LABEL = 1;
  public static final int IDX_XID = 2;
  public static final int IDX_PI = 3;

  // Always chained
  // Key is just a workaround to send to single reducer
  public static final int IDX_OUT_DIMENSION = ReduceFlattenToVector.IDX_DIMENSION;
  public static final int IDX_OUT_COEFICCIENT = ReduceFlattenToVector.IDX_DOUBLE_VALUE;
  public static final int IDX_OUT_KEY_CONST_ONE = ReduceFlattenToVector.IDX_KEY_CONST_ONE;

  public static final String CONF_KEY_NEWTON_MAX_ITERATIONS = "train.newton-max-iterations";
  public static final String CONF_KEY_NEWTON_TOLERANCE = "train.newton-tolerance";
  public static final String CONF_KEY_REGULARIZATION = "train.regularization";
  
  private int maxIterations;
  private double lambda;
  private double tolerance;
  
  // ATTENTION: This does not make the trained coefficients vector smaller - we always allocate a dense vector with numDimensions!
  private static final int THRESHOLD_MIN_NUM_RECORDS = 0;
  
//  private static final Log logger = LogFactory.getLog(TrainDimensions.class);
  
  private final PactRecord recordOut = new PactRecord(3);
  
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.maxIterations = parameters.getInteger(CONF_KEY_NEWTON_MAX_ITERATIONS, -1);
    if (this.maxIterations == -1) {
      throw new RuntimeException("Value for the configuration parameter CONF_KEY_NEWTON_MAX_ITERATIONS is not defined, please set it in plan assembler");
    }
    // TODO: BUG: parameters.getDouble always returns default value
    this.tolerance = Double.parseDouble(parameters.getString(CONF_KEY_NEWTON_TOLERANCE, "-1"));
    if (this.tolerance == -1) {
      throw new RuntimeException("Value for the configuration parameter CONF_KEY_NEWTON_TOLERANCE is not defined, please set it in plan assembler");
    }
    this.lambda = Double.parseDouble(parameters.getString(CONF_KEY_REGULARIZATION, "-1"));
    if (this.lambda == -1) {
      throw new RuntimeException("Value for the configuration parameter CONF_KEY_REGULARIZATION is not defined, please set it in plan assembler");
    }
  }

  @Override
  public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out)
      throws Exception {
    
    List<NewtonSingleFeatureOptimizer.Record> cache = Lists.newArrayList();
    
    // Cache all records
    PactRecord record = null;
    while (records.hasNext()) {
      record = records.next();
      cache.add(new NewtonSingleFeatureOptimizer.Record(
          record.getField(IDX_XID, PactDouble.class).getValue(), 
          record.getField(IDX_LABEL, PactInteger.class).getValue(), 
          record.getField(IDX_PI, PactDouble.class).getValue()));
    }
    
    if (cache.size() < THRESHOLD_MIN_NUM_RECORDS) {
      return;
    }
    
    // Train single dimension using Newton Raphson
    double betad = NewtonSingleFeatureOptimizer.train(cache, maxIterations, lambda, tolerance);
    
    recordOut.copyFrom(record, new int[] {IDX_DIMENSION}, new int[] {IDX_OUT_DIMENSION});
    recordOut.setField(IDX_OUT_KEY_CONST_ONE, PactUtils.pactOne);
    recordOut.setField(IDX_OUT_COEFICCIENT, new PactDouble(betad));
    out.collect(recordOut);
  }

}
