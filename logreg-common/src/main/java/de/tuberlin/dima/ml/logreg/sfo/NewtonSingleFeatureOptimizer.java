package de.tuberlin.dima.ml.logreg.sfo;

import java.util.List;

import de.tuberlin.dima.ml.logreg.LogRegMath;

/**
 * Newton-Raphson Optimizer for logistic regression, which optimizes only a
 * single dimension that is being added to an existing model, according to SFO
 * (See {@link SFODriver}). This means all coefficients except the new one are
 * held fixed. It maximizes log-likelihood.
 * 
 * The training can be executed based on a list of pre-aggregated values, which
 * are stored in Record objects. The pre-aggregation makes use of the sparseness
 * of the input.
 * 
 * @author André Hacker
 */
public class NewtonSingleFeatureOptimizer {
  
  /**
   * Train a single dimension based on pre-aggregated values.
   * 
   * @param cache
   *          list of pre-aggregated records, representing the input to be
   *          learned from
   * @param maxIterations
   *          maximum number of newton-raphson iterations
   * @param lambda
   *          L2-regularization penalty term for Newton-Raphson. Set to 0 for no
   *          regluarization. Higher values result in higher regularization.
   * @param tolerance
   *          The tolerance criteria for determine convergence. Convergence will
   *          be assumed if the change in the trained coefficient is smaller
   *          than the tolerance.
   * @return the new trained coefficient
   */
  public static double train(List<Record> cache, int maxIterations, double lambda, double tolerance) {
    double betad = 0;
    int iteration = 0;
    double lastUpdate = 0;
    boolean converged = false;
    while ((++iteration <= maxIterations) && !converged) {

      double batchGradient = 0;
      double batchGradientSecond = 0;
      for (Record element : cache ) {

        double xDotw = Math.log(element.getPi() / (1 - element.getPi()));

        double piNew = LogRegMath.logisticFunction(
            xDotw + (element.getXid() * betad));

        batchGradient += LogRegSFOTraining.derivateL2SFO(element.getXid(),
            piNew, element.getYi(), lambda, betad);
        batchGradientSecond += LogRegSFOTraining.derivateSecondL2SFO(
            element.getXid(), piNew, lambda);
      }

      if (batchGradientSecond == 0) {
        lastUpdate = 0;
      } else {
        lastUpdate = (batchGradient / batchGradientSecond);
        betad -= lastUpdate;
      }

      if (Math.abs(lastUpdate) < tolerance) {
        converged = true;
      }
    }
    return betad;
  }
  
  /**
   * A pre-aggregated representation of a single input record. To train a single
   * dimension we don't need the full input vector and instead we can work with
   * this pre-aggreagated values. Please see the Singh et al. paper for details (see
   * {@link SFODriver}).
   */
  public static final class Record {
    
    private double xid;
    private int yi;
    private double pi;
    
    public Record(double xid, int yi, double pi) {
      this.xid = xid;
      this.yi = yi;
      this.pi = pi;
    }
    
    public double getXid() { return xid; }
    public int getYi() { return yi; }
    public double getPi() { return pi; }
  }

}
