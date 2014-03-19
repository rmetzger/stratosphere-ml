package bigdataproject.scala.eu.stratosphere.test

import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import org.junit.rules.TemporaryFolder
import java.io.{File, PrintWriter}

class testRandomForest extends JUnitSuite  {
  var folder : TemporaryFolder = null
  var numberTrees = 1
  var testFolder : String = ""
  var testEvaluationSet : String = ""
  var testTrainingSet : String = ""

  @Before
  def prepare = {
    System.out.println("prepare")
    folder = new TemporaryFolder()
    folder.create()
    System.out.println("create testfolder "+folder.getRoot.toString )
    testFolder = folder.getRoot.toString+"/"
    testEvaluationSet = testFolder+"mnist8m.test.evalset"
    testTrainingSet = testFolder+"mnist8m.test.dataset"

  }
  @Test
  def testBuilding {
    val outTrainSet = new PrintWriter(new File(testTrainingSet));
    outTrainSet.write(
      io.Source.fromInputStream(getClass.getResourceAsStream("/bigdataproject/scala/eu/stratosphere/test/mnist8m.test.dataset")).mkString
    )
    outTrainSet.close()

    val outEvalSet = new PrintWriter(new File(testEvaluationSet));
    outEvalSet.write(
      io.Source.fromInputStream(getClass.getResourceAsStream("/bigdataproject/scala/eu/stratosphere/test/mnist8m.test.dataset")).mkString
    )
    outEvalSet.close()

    new RandomForestBuilder().build(
      testFolder,
      testTrainingSet,
      numberTrees,
      "streaming"
    )

    val percentage =
      new RandomForestBuilder().eval(
        testEvaluationSet,
        testFolder+"rf_output_tree",
        testFolder+"rf_output_evaluation"
      )

  }


  @After
  def cleanup = {
    System.out.println("cleanup")
    folder.delete()
  }

}
