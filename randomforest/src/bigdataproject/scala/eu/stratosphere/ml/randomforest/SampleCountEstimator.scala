package bigdataproject.scala.eu.stratosphere.ml.randomforest
package bigdataproject.scala.eu.stratosphere.ml.randomforest


import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

import java.io._
import scala.util.Random


class SampleCountEstimator extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath]"
  }
  override def getPlan(args: String*) = {
    val newLine = System.getProperty("line.separator");
    val inputPath = DecisionTreeUtils.preParseURI(args(0))
    val outputPath = DecisionTreeUtils.preParseURI(args(1))
    val trainingSet = TextFile(inputPath)
    val samples = trainingSet map { line => (new Random().nextInt(10), 1) } groupBy ({ x => x._1 }) reduce({ (x,y)=>(0, x._2+y._2) }) map (x=>x._2)
    val totalCountSink = samples.write(outputPath, CsvOutputFormat(newLine, ","))
    new ScalaPlan(Seq(totalCountSink))
  }
}