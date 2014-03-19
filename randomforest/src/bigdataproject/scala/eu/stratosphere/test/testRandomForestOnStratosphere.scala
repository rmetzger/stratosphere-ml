package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder

object testRandomForestOnStratosphere {
 
  def main(args: Array[String]) { 
	new RandomForestBuilder().build(
	    "/home/kay/rf/",
      "/home/kay/Dropbox/kay-rep/Uni-Berlin/MA_INF_Sem3_WS13/BigDataAnalytics/datasets/normalized.txt",
	    1
	    )
  }
 
}