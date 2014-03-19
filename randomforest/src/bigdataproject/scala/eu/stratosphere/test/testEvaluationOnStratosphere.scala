package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder

object testEvaluationOnStratosphere {
 
  def main(args: Array[String]) { 
	new RandomForestBuilder().eval(
	    "/home/kay/Dropbox/kay-rep/Uni-Berlin/MA_INF_Sem3_WS13/BigDataAnalytics/datasets/normalized_full_test.txt",
	    "/home/kay/rf/rf_output_tree",
	    "/home/kay/rf/rf_output_evaluation"
	    )
  }
}