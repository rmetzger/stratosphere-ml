package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.utils.MNISTParser
import mnist.tools.MnistManager

object testGenerateStratosphereInputFromMNIST {
  def main(args: Array[String]) {
	val p = new MNISTParser;
    
  	p.saveData(new MnistManager( "/home/kay/train-images.idx3-ubyte",
		  					"/home/kay/train-labels.idx1-ubyte"),
		  					"/home/kay/normalized.txt", 5000)
		  					
  	p.saveData(new MnistManager( "/home/kay/t10k-images.idx3-ubyte",
		  					"/home/kay/t10k-labels.idx1-ubyte"),
		  					"/home/kay/normalized_test.txt", 100)
  }
  
}