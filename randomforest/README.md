Random Forest implentation for Stratosphere
==============

Creating the random forest
--------------

*Some comment*

**Data format**

Test data should be separated by a new line. Each line should be formatted accordingly:

[zero based line index] [label] [feature 1 value] [feature 2 value] .. [feature N value]

**Usage with LocalExecutor**

	/**
	 * Builds a random forest model based on the training data set. Iteratively executes a new
	 * [[bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder]] for every level of
	 * the forest, in case there are nodes to split on that level.
	 * 
	 * @param outputPath Folder that will contain the output model at outputPath\rf_output_tree
	 * 
	 * @param dataFilePath Test data set. Format:
	 * [zero based line index] [label] [feature 1 value] [feature 2 value] [feature N value]
	 * 
	 * @param numTrees Number of trees in the forest
	 */
	new RandomForestBuilder().build([outputPath], [dataFilePath], [numTrees])

**Usage on a cluster**

	
	java -cp /stratosphere-path/stratosphere/lib/*:stratosphere_randomforest.jar bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestExecutor build dest-path data-src numb-trees [remoteJar remoteHost remotePort]
	

Using the random forest to evaluate/classify data
--------------

**Data format**

Test data should be in the same format as for creating the random forest.

**Usage with LocalExecutor**

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
	new RandomForestBuilder().eval([inputPath], [treePath], [outputPath])

**Usage on a cluster**


	java -cp /stratosphere-path/stratosphere/lib/*:stratosphere_randomforest.jar bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestExecutor eval dest-path data-src [remoteJar remoteHost remotePort]



