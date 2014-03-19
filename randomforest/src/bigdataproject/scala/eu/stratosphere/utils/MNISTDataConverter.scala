package bigdataproject.scala.eu.stratosphere.utils

import scala.io.Source
import scala.Array
import java.io.PrintWriter
// import following package
import scala.util.control._

/**
 * Created by kay on 2/9/14.
 */
object MNISTDataConverter {
  def main(args: Array[String]) {
    convertfromSVMlibtoOwn( "/home/kay/Desktop/mnist8m.raw",
                            "/home/kay/Desktop/mnist8m.test.dataset",
                            1000 )
    convertfromSVMlibtoOwn( "/home/kay/Desktop/mnist8m.raw",
                            "/home/kay/Desktop/mnist8m.test.evalset",
                            100,
                            1000
                        )
  }

  def convertfromSVMlibtoOwn(input : String, output : String, count : Int, start : Int = 0 ){
    var counter=0
    val out = new PrintWriter(output);
    val loop = new Breaks;
    var line_number = 0
    loop.breakable{
      for(  line <- Source.fromFile(input).getLines ){
        val values = line.split(" ")
        val label = values.head
        val randomCount = 784
        var stop = false
        var arr: Array[Double] = Array()
        arr = Array(784)
        arr = Array.fill(randomCount)(0)
        for( v <- values.tail ){
          arr( v.split(":")(0).toInt-1 ) = v.split(":")(1).toDouble / 255.0
        }
        if( label.toInt == 1 || label.toInt == 2 ) {
          if( counter <= start+count && counter >= start ){
            var text = ""
            if (counter > start )
              text += "\r\n"
            text += counter + " " + label + " " + arr.mkString(" ")
            out.print(text);
          }
          if(counter >= start+count){
            System.out.println("stop")
            loop.break;
          }
          if(counter % 1000 == 0 ){
            System.out.println(counter)
          }//if

          counter = counter +1
        }
        line_number = line_number +1
      }
    }
    out.close()
  }
}
