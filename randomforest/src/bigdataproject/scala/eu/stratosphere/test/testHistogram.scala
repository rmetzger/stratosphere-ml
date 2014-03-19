package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram
import org.junit.Test
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import scala.util.Random

class testHistogram extends JUnitSuite  {

  @Test
  def testHistogram_paper = {
	 val h = new Histogram(2,5)
	 h.update(23.0)
	 h.update(19.0)
	 h.update(10.0)
	 h.update(16.0)
	 h.update(36.0)
   assertTrue(  h.getBins.toList == List( (10.0,1), (16.0,1), (19.0,1), (23.0,1), (36.0,1) ) )

   h.update(2.0)
   assertTrue(  h.getBins.toList == List((2.0,1), (10.0,1), (17.5,2), (23.0,1), (36.0,1)) )

	 h.update(9.0)
   assertTrue(  h.getBins.toList == List((2.0,1), (9.5,2), (17.5,2), (23.0,1), (36.0,1)) )

	 h.update(32.0)
   assertTrue(  h.getBins.toList == List((2.0,1), (9.5,2), (17.5,2), (23.0,1), (34.0,2)) )

	 h.update(30.0)
   assertTrue(  h.getBins.toList == List((2.0,1), (9.5,2), (17.5,2), (23.0,1), (32.666666666666664,3)) )
  }

  @Test
  def testHistogram_serialization = {
    val h = new Histogram(2,10)
    (0 until 20000).foreach({ x =>
      val max = Random.nextDouble * (Random.nextInt(60) - 30)
      h.update(max)
    })
    assertTrue( Histogram.fromString( h.toString ).toString == h.toString )
  }
}