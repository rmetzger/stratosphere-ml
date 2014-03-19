package bigdataproject.scala.eu.stratosphere.utils
import java.io.PrintWriter
import mnist.tools.MnistManager

class MNISTParser {
  def saveData(m : MnistManager, outPath : String, count : Int): Unit = {
    
    var labels = m.getLabels()
    var num_samples = labels.getCount() 
    System.out.println( "samples found " + labels.getCount() )
    System.out.println( "image-format " + m.getImages().getCols() + " cols, " + m.getImages().getRows() + " rows" )
    System.out.println();
    var out = new PrintWriter(outPath);
    var index = 0;
    for (i <- 0 until num_samples) {
    	//System.out.println("read sample "+i)
    	val image = m.readImage()
    	val label = m.readLabel()

    	if (true)
    	{
    		var text = ""
			 if (index > 0)
			   text += "\r\n"
    		text += index + " " + label + " " + image.map(line => line.map(arv => arv.toDouble / 256.toDouble).mkString(" ")).mkString(" ")
    		out.print(text);
    		index = index + 1;
    	}
    }
    out.close()
  }
}