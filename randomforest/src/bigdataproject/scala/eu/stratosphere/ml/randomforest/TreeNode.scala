package bigdataproject.scala.eu.stratosphere.ml.randomforest
import collection.mutable.HashMap
case class TreeNode ( 
	treeId : Long,
	nodeId : Long,
		
	// list of features total available for this level
	val features : Array[Int],
	// list of features left for random feature-selection (m) - subset of "features"
	featureSpace : Array[Int],
	// -1 if not set
	splitFeatureIndex : Int,
	// -1 if not set
	splitFeatureValue : Double,
	// -1 if not set
	label : Int
	){
}