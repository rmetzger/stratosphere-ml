package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Plan
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._


import scala.util.matching.Regex
import util.Random
  
/**
 * Classifies the data set based on the random forest model.
 * Outputs contains a line for every classification:
 * "[data item index], [classified label], [actual label from data item]"
 */
class DecisionTreeEvaluator() extends Program with ProgramDescription with Serializable {

  
  override def getDescription() = {
	  "Usage: [inputPath] [treePath] [outputPath]"
  }
  
  /**
   * @param inputPath Data to classify/evaluate. In the same format as training data.
   * 
   * @param treePath The random forest model, created by
   * [[bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder]].build()
   * 
   * @param outputPath Classified data, separated by a newline with the format:
   * "[data item index], [classified label], [actual label from data item]"
   */
  override def getPlan(args: String*) = {
    val inputPath = DecisionTreeUtils.preParseURI(args(0))
    val treePath = DecisionTreeUtils.preParseURI(args(1))
    val outputPath = DecisionTreeUtils.preParseURI(args(2))
    
    val inputFile = TextFile(inputPath)
    val treeFile = TextFile(treePath)
    
    val treeNodes = treeFile
    		.map({nodedata =>
		        val treeId = nodedata.split(",")(0)	        
		        (treeId,nodedata)
	      		})
	      	.groupBy( _._1 )
	      	.reduceGroup( values => {
	      		val buffered = values.buffered.toList
	      		val treeId = buffered.head._1
	      		val nodes = buffered;
	      		(nodes.map({x=>x._2}).mkString(";"))
	      	} )
    val treeEvaluations = inputFile
    			.cross(treeNodes)
    			.map((line, tree) => {    				
				 val nodes = tree.split(";").map(node => {
				                val nodeData = node.split(",").map(_.trim())
				                new TreeNode(nodeData(0).toLong, nodeData(1).toLong, null, null, nodeData(2).toInt, nodeData(3).toDouble, nodeData(4).toInt)
				              })				          
		  val values = line.split(" ")
		  val index = values.head.trim().toInt
		  val label = values.tail.head.trim().toInt
		  val features = values.tail.tail
	      
		  var currentNodeIndex : Long = 0;
	      var labelVote = -1;
	      do
	      {
			  val currentNode = nodes.find(_.nodeId == currentNodeIndex).orNull
			  labelVote = currentNode.label
			  
			  if (labelVote == -1)
			  {
			    //right child:
			    currentNodeIndex = ((currentNode.nodeId + 1) * 2)
			    
			    //left child:
				if (features(currentNode.splitFeatureIndex).toDouble <= currentNode.splitFeatureValue)
				    currentNodeIndex -= 1
			  }
	    	  
	      }while (labelVote == -1)
		  
	      (index,labelVote,label)
	    })
	    
	val forestEvaluations = treeEvaluations
		.filter(_._2 > -1)
		.groupBy(_._1)
		.reduceGroup( values => {
			val buffered = values.buffered.toList
			val dataItemIndex = buffered.head._1
			val actualLabel = buffered.head._3
			val winningLabelGuess = buffered
				.groupBy(_._2)
				.map(labelOccurance => (labelOccurance._1, labelOccurance._2.length))
				.maxBy(_._2)._1
			
			//group by label, then count occurances
			(dataItemIndex, winningLabelGuess, actualLabel)
		})
	  
    val sink = forestEvaluations.write(outputPath, CsvOutputFormat("\n",","))
    new ScalaPlan(Seq(sink))
  }
}
