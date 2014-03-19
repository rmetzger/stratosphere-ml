package bigdataproject.scala.eu.stratosphere.ml.randomforest


import eu.stratosphere.client.LocalExecutor
import scala.util.Random
import java.io._
import scala.io.Source
import scala.collection.mutable.Buffer
import org.apache.log4j.Level
import eu.stratosphere.client.PlanExecutor
import eu.stratosphere.client.RemoteExecutor
import eu.stratosphere.core.fs.FileSystem
import eu.stratosphere.core.fs.Path
import bigdataproject.scala.eu.stratosphere.ml.randomforest.SampleCountEstimator
import java.net.URI
import eu.stratosphere.api.common.Plan


/**
 * Functionality to build and evaluate a random forest.
 * 
 * @param remoteJar When set, uses [[eu.stratosphere.client.RemoteExecutor]] to execute functionality.
 * 
 * @param remoteJobManager Stratosphere remote job manager URI
 * 
 * @param remoteJobManagerPort Stratosphere remote job manager port
 */
class RandomForestBuilder(val remoteJar : String = null,
                          val remoteJobManager : String = null,
                          val remoteJobManagerPort : Int = 0 ) {

	/**
	 * Utility method to get the total sample count (for creating bagging tables)
	 */
	private def getSampleCount( ex : PlanExecutor, filename: String, outputPath : String ): Int = {
		val outputSampleCountPath = outputPath+"/rf_samples_count"
		val plan = new SampleCountEstimator().getPlan( filename, outputSampleCountPath )
		val runtime = ex.executePlan(plan)

    var count=0
    val lines = mergeOutputResults(outputSampleCountPath)
    for( l <- lines )
      count += l.toInt
   count
	}

  /**
  * Utility method to get the total path size (supporting one level depth)
  */
  private def getPathSize( dir : String ) = {
    val fs : FileSystem = FileSystem.get(new URI(dir))
    val fileDetails = fs.getFileStatus(new Path(dir))
    if( !fileDetails.isDir ){
      val stream = Source.fromInputStream(fs.open(new Path(new URI(dir))))
      val output = stream.size
      stream.close()
      output
    }else {
      var size : Int = 0
      for( file <- fs.listStatus(new Path(new URI(dir))) ){
        val stream = Source.fromInputStream(fs.open(file.getPath))
        size = size + stream.size
        stream.close()
      }

      size
    }
  }

  /**
   * Utility method to merge  many output files from a reducer
   */
  private def mergeOutputResults( outputDir : String ) = {
    val fs : FileSystem = FileSystem.get(new URI(outputDir))
    val fileDetails = fs.getFileStatus(new Path(outputDir))
    if( !fileDetails.isDir ){
      Source.fromInputStream(fs.open(new Path(new URI(outputDir)))).getLines
    }else {
      val lines = scala.collection.mutable.MutableList[String]()
      for( file <- fs.listStatus(new Path(new URI(outputDir))) ){
        val stream = Source.fromInputStream(fs.open(file.getPath))
        for( l <- stream.getLines() ){
          lines.+=( l )
        }
        stream.close()
      }
      lines.toList
    }
  }


  /**
	 * Utility method to get the feature count
	 */
	private def getFeatureCount(filename: String): Int = {
	    val fs : FileSystem = FileSystem.get(new URI(filename))
	    val is : InputStream = fs.open(new Path(filename) )
	    val br : BufferedReader = new BufferedReader(new InputStreamReader(is))
	    val line=br.readLine()
		try {
			line.split(" ").tail.tail.size
		} finally {
		}
	}

	/** 
	* Evaluates test data set based on the random forest model.
	* 
	* @param inputPath Data to classify/evaluate. In the same format as training data.
	* 
	* @param treePath The random forest model, created by
	* [[bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder]].build()
	* 
	* @param outputPath Classified data; format:
	* "[data item index], [classified label], [actual label from data item]"
	*/
	def eval(inputFile: String, treeFile: String, outputFile: String) = {
	  val fs : FileSystem = FileSystem.get(new URI(inputFile))
	  val inputPath = inputFile
		val treePath = treeFile
		val outputPath = outputFile

		// prepare executor
	    var ex : PlanExecutor = null
	    if( remoteJar == null ){
	      val localExecutor = new LocalExecutor();
	      localExecutor.start()
	      ex = localExecutor
	      LocalExecutor.setLoggingLevel(Level.ERROR)
        println("Stratosphere using local executor")
	    } else {
	      ex = new RemoteExecutor(remoteJobManager, remoteJobManagerPort, remoteJar );
        println("Stratosphere using remotee xecutor ip:"+remoteJobManager+" port:"+remoteJobManagerPort+" jar:"+remoteJar)
	    }

		val plan = new DecisionTreeEvaluator().getPlan(inputPath, treePath, outputPath)
		val runtime = ex.executePlan(plan)

    var percentage = 0.0
		val src =Source.fromInputStream(fs.open(new Path(new URI(outputFile))))
		try {
			val lines = src.getLines.map(_.split(",").map(_.toInt)).toList

			System.out.println("statistics");
			System.out.println("total results: " + lines.length)
			val correct = lines.filter(x => x(1) == x(2)).length
			System.out.println("correct: " + correct)
			val wrong = lines.filter(x => x(1) != x(2)).length
			System.out.println("wrong: " + wrong)
			System.out.println("percentage: " + (correct.toDouble * 100 / lines.length.toDouble))
      percentage = (correct.toDouble * 100 / lines.length.toDouble)
    } finally {
			src.close()
		}
    percentage
	}

	/**
	 * Builds a random forest model based on the training data set. Iteratively executes a new
	 * [[bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder]] for every level of
	 * the forest, in case there are nodes to split on that level.
	 * 
	 * @param outputPath Folder that will contain the output model at outputPath\rf_output_tree
	 * 
	 * @param inputPath Test data set. Format:
	 * [zero based line index] [label] [feature 1 value] [feature 2 value] [feature N value]
	 * 
	 * @param numTrees Number of trees in the forest
	 */
	def build(outputPath: String, inputPath: String, numTrees: Int, build_strategy : String = "streaming") : Any = {
		build(outputPath, inputPath, outputPath + "rf_input_nodequeue", outputPath + "rf_output", outputPath + "rf_output_tree", numTrees, build_strategy)
	}
	
	private def build(outputPath: String, inputPath: String, inputNodeQueuePath: String, outputNodeQueuePath: String, outputTreePath: String, numTrees: Int, build_strategy : String) : Any = {
	  // prepare executor
	  var ex : PlanExecutor = null
	  if( remoteJar == null ){
	    val localExecutor = new LocalExecutor();
	    localExecutor.start()
	    ex = localExecutor
	    LocalExecutor.setLoggingLevel(Level.ERROR)
      System.out.println("Stratosphere using local executor")
    } else {
	    ex = new RemoteExecutor(remoteJobManager, remoteJobManagerPort, remoteJar );
      System.out.println("Stratosphere using remote executor ip:"+remoteJobManager+" port:"+remoteJobManagerPort+" jar:"+remoteJar)
	  }

    // dependent on the path decription choose the correct filesystem to work with
    val fileSystem : FileSystem = FileSystem.get( new URI(outputPath) )
		val newLine = System.getProperty("line.separator");

		// start measuring time
		val t0 = System.currentTimeMillis


    var nodesQueue = Buffer[TreeNode]()
		val totalFeatureCount = getFeatureCount(inputPath)
		val featureSubspaceCount = Math.round(Math.log(totalFeatureCount).toFloat + 1);

    System.out.println("total feature count:"+totalFeatureCount)


		// add node to build for each tree
		val sampleCount = getSampleCount(ex, inputPath, outputPath)
    System.out.println("samples "+sampleCount)
		for (treeId <- 0 until numTrees) {
			// TODO: the features left is the whole set minus still used best-splits
			val features = (0 until totalFeatureCount).toArray

			// randomized
			val featureSubspace = DecisionTreeUtils.generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
			nodesQueue += new TreeNode(treeId, 0, features, featureSubspace, -1, -1, -1)
		} //for

		// write the initial nodes to file to join in the iteration
		writeNodes(nodesQueue, new URI(inputNodeQueuePath), sampleCount);

		// if next level, read from file which node has to be split
		// each line treeId,nodeId, featuresIndicies, baggingTable

		var nodeQueueSize = 0
		var level = 0
		var totalNodes = nodesQueue.length

		// do some cleanup stuff
		fileSystem.delete(new Path(new URI(outputTreePath)), false )

		val level_outputTreePath = outputTreePath + "CurrentLevel"
		val treeStream : OutputStream = fileSystem.create(new Path(outputTreePath), true )
		val treeStreamWriter = new OutputStreamWriter(treeStream)
		
		do {
      System.out.println("Iteration "+level)
      var plan : Plan = null
      if (build_strategy == "streaming") {
        plan = new DecisionTreeBuilderStreaming(70, featureSubspaceCount, level ).getPlan(
                  inputPath,
                  inputNodeQueuePath,
                  outputNodeQueuePath,
                  level_outputTreePath,
                  outputPath )
      }
      else if (build_strategy == "bigreducer") {
        plan = new DecisionTreeBuilderBigReducer(70, featureSubspaceCount, level ).getPlan(
          inputPath,
          inputNodeQueuePath,
          outputNodeQueuePath,
          level_outputTreePath,
          outputPath )
      } else {
        System.out.println("ERROR: unknown decision tree build strategy")
        System.exit(0)
      }

			val runtime = ex.executePlan(plan)
			
			// delete old input node queue
			fileSystem.delete(new Path(new URI(inputNodeQueuePath)), true )


			// change output nodequeue to input queue
			fileSystem.rename(new Path(new URI(outputNodeQueuePath)), new Path(new URI(inputNodeQueuePath)))

			// check how many nodes to build
			nodeQueueSize = mergeOutputResults(inputNodeQueuePath).size


			totalNodes += nodeQueueSize

			var treeData = ""
			treeData = mergeOutputResults(level_outputTreePath).mkString(newLine)

      System.out.println(treeData)
			
      treeStreamWriter.write(treeData)
      treeStreamWriter.write(newLine)
      treeStreamWriter.flush()

			// delete temporal file
			fileSystem.delete(new Path(new URI(level_outputTreePath)), true )

			// increment for next level
			level = level + 1;

		} while (nodeQueueSize > 0)

    treeStreamWriter.close()

		// stop measuring time
		val t1 = System.currentTimeMillis

    System.out.println("statistics");
    System.out.println("build-time: " + ((t1 - t0) / 1000.0) / 60.0 + "mins")
    System.out.println("samples: " + sampleCount)
    System.out.println("features per sample: " + totalFeatureCount)
    System.out.println("trees: " + numTrees)
    System.out.println("tree-levels (iterations): " + (level - 1))
	}
	
	/**
	 * Write node-queue efficiently to file.
	 * Line format: treeID, nodeId, baggingTable, featureSpace, features
	 */
	private def writeNodes(nodes: Buffer[TreeNode], outputPath: URI, baggingTableSize : Int) {
	    val fs : FileSystem = FileSystem.get(outputPath)
	    val os : OutputStream = fs.create(new Path(outputPath), true )
		val newLine = System.getProperty("line.separator");
    	val fw = new OutputStreamWriter(os)
		try {
			for (i <- 0 until nodes.length) {
				val node = nodes(i)
		        fw.write(node.treeId + ",")
		        fw.write(node.nodeId + ",")
		        fw.write(node.splitFeatureIndex + ",")
		        fw.write(node.splitFeatureValue + ",")
		        fw.write(node.label + ",")
				
				for (i <- 0 until baggingTableSize){
					fw.write(Random.nextInt(baggingTableSize) + " ")
				}
        fw.write(",")
        fw.write(node.featureSpace.mkString(" ") + ",")
        fw.write(node.features.mkString(" "));
				if (i != nodes.length - 1)
					fw.write(newLine)
			}
		} finally {
			fw.close()
		}
	}

}