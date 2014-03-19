package bigdataproject.scala.eu.stratosphere.ml.randomforest
import scala.collection.mutable.Buffer
import scala.util.Random
import java.io.File

object DecisionTreeUtils {
  def generateFeatureSubspace(randomCount : Int, maxRandomNumber : Int) : Array[Int] = {
	var features = Buffer[Int]();
	// Generate an arrayList of all Integers
	for(i <- 0 until maxRandomNumber){
	    features += i;
	}
	generateFeatureSubspace(randomCount, features)
  }
  
  def generateFeatureSubspace(randomCount : Int, features : Buffer[Int]) : Array[Int] = {
    var arr : Array[Int] = Array()
	arr = Array(randomCount)
	arr = Array.fill(randomCount)(0)
	for(i <- 0 until randomCount)
	{
	    var random = new Random().nextInt(features.length);
	    arr(i)=features.remove(random);
	}
	arr;
  }
  
  var preParseURIForLocalFileSystem = false
  
  /**
   * Useful when testing on a Windows system
   */
  def preParseURI(uri : String) : String = {
  	if (preParseURIForLocalFileSystem)
  		new File(uri).toURI().toString()
  	else
  		uri
  }
}