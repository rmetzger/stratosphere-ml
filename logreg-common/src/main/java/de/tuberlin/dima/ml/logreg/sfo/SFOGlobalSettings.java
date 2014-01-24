package de.tuberlin.dima.ml.logreg.sfo;


/**
 * Static settings for SFO
 * 
 * The goal is to remove all those global static settings and load them at runtime
 */
public class SFOGlobalSettings {
  
  // We used 1 for most experiments. Singh does not tell what they set the bias to.
  // This has an impact on the gain computation!
  // Bias=0 yields prediction 0.5 for empty models, bias=1 yields 0.731... and -1 yields 0.268..
  // Liblinear uses constant bias -1 according to the model file
  public static final double INTERCEPT = 1;
  
}
