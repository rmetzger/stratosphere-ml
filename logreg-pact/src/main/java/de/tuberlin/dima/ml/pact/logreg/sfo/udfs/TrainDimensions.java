package de.tuberlin.dima.ml.pact.logreg.sfo.udfs;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.logreg.sfo.NewtonSingleFeatureOptimizer;
import de.tuberlin.dima.ml.pact.udfs.ReduceFlattenToVector;
import de.tuberlin.dima.ml.pact.util.PactUtils;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


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
public class TrainDimensions extends ReduceFunction {
  
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
  
  private final Record recordOut = new Record(3);
  
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
  public void reduce(Iterator<Record> records, Collector<Record> out)
      throws Exception {
    
    List<NewtonSingleFeatureOptimizer.Record> cache = Lists.newArrayList();
    
    // Cache all records
    Record record = null;
    while (records.hasNext()) {
      record = records.next();
      cache.add(new NewtonSingleFeatureOptimizer.Record(
          record.getField(IDX_XID, DoubleValue.class).getValue(),
          record.getField(IDX_LABEL, IntValue.class).getValue(),
          record.getField(IDX_PI, DoubleValue.class).getValue()));
    }
    
    if (cache.size() < THRESHOLD_MIN_NUM_RECORDS) {
      return;
    }
    
    // Train single dimension using Newton Raphson
    double betad = NewtonSingleFeatureOptimizer.train(cache, maxIterations, lambda, tolerance);
    
    recordOut.copyFrom(record, new int[] {IDX_DIMENSION}, new int[] {IDX_OUT_DIMENSION});
    recordOut.setField(IDX_OUT_KEY_CONST_ONE, PactUtils.pactOne);
    recordOut.setField(IDX_OUT_COEFICCIENT, new DoubleValue(betad));
    out.collect(recordOut);
  }

}
