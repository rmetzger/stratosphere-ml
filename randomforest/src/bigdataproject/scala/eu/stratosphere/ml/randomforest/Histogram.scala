package bigdataproject.scala.eu.stratosphere.ml.randomforest

case class Histogram(
		feature : Integer,
		maxBins : Integer,
		var bins : scala.collection.mutable.Buffer[(Double,Int)] = scala.collection.mutable.Buffer[(Double,Int)](),
		var maxBinValue : Double = Double.NegativeInfinity,
		var minBinValue : Double = Double.PositiveInfinity
	) {
  def getBins = bins
 
  def getMax = maxBinValue
  def getMin = minBinValue
  def getNormalSum = bins.map(_._2).sum
  def getNormalSum( x : Double) = bins.filter(d=>d._1 > x).map(_._2).sum

  //TODO: Bug fix
  def uniform( Bnew : Integer ) = {
   val u = scala.collection.mutable.Buffer[Double]()
   if( Bnew > bins.length )
     u.toList
    else{
     val sums = bins.map(x=>sum(x._1))
     val binSum = bins.map(_._2).sum
     for(j <- 1 until Bnew){   
      val s= (j.toDouble/Bnew) * binSum.toDouble
      var i = sums.filter( x=>(x<s) ).length-1
      if(i<0)i=0 // this is important if the first bucket is very large
      val d = math.abs(s - sums(i))
      val pi = bins(i)      
      val pi1 = bins(i+1)
      val a = math.max( pi1._2 - pi._2, 0.00000001)
      val b = 2*pi._2.toDouble
      val c = -2*d
      val z = (-b + math.sqrt(b*b - 4*a*c)) / (2*a);
      val uj = pi._1+(pi1._1 - pi._1)*z
      u += uj
     }
     u.toList
    }
  }
  
  def sum(b:Double) = {
   if(b>=maxBinValue) {
     bins.map(_._2).sum
   }
   else if(b<minBinValue)
     0
   else if(b==minBinValue)
     bins.head._2
   else {
     sum_of_bin(b)
   }
  }
  
  private def sum_of_bin(b:Double ) = {
     val pos =  bins.zipWithIndex.filter( x=> x._1._1 < b )
     if(pos.length==0)
      bins.head._2
     else {
       val i = pos.last._2
       val bi = bins(i)
       val bi1 = bins(i+1)
       val mb =  bi._2 + ((bi1._2-bi._2)/(bi1._1-bi._1)) * (b - bi._1 ) 
       var s = ((bi._2+mb) / 2) * ((b - bi._1 )/(bi1._1-bi._1))
       for (j <- 0 until i)  s += bins(j)._2
       s = s + bins(i)._2.toDouble / 2.0
       s
     }
  }
   
  def merge(h:Histogram) = {
    val h2 = new Histogram(feature,maxBins)
    for(i<-0 until bins.length) h2.update(bins(i)._1, bins(i)._2)
    for(i<-0 until h.bins.length) h2.update(h.bins(i)._1, h.bins(i)._2)
    h2
  }  
  def update ( p : Double ) : this.type = {
    update(p,1)
    this
  }
  def update ( p : Double, c : Int ) : this.type = {
    var bin = bins.zipWithIndex.find(pm => pm._1._1 == p)
    if( bin != None )
      bins(bin.head._2) = (bin.head._1._1,bin.head._1._2+c)
    else{
     bins +=( (p, c) )
     sort
     compress_one
     maxBinValue=bins.map(_._1).max
     minBinValue=bins.map(_._1).min
     //math.min(minBinValue, p)
    }
    this
  }
  private def sort {
     bins=bins.sortWith( (e1, e2) => e1._1 <= e2._1 )
  }
  private def compress_one {
    // only compress if the numer of elements exeeds 
    // the maximum bins allowed
    if( bins.length > maxBins ){
      val q = bins.take(bins.length-1).zip(bins.tail).zipWithIndex.sortWith( (x,y) => (x._1._1._1-x._1._2._1)  > (y._1._1._1-y._1._2._1) ).head._2
      val qi = bins.remove(q)
      val qi1 = bins(q)
      bins(q) = ( (qi._1*qi._2 + qi1._1*qi1._2)/(qi._2+qi1._2), qi._2+qi1._2)
    }
  }
  override def toString = {
    feature+";"+maxBins+";"+bins.map(x=>""+x._1+" "+x._2).mkString(",")
  }
  
  def print = {
    System.out.println(toString);
    val normalsum = getNormalSum
	System.out.println("Histogram "+feature+", "+maxBins);
	System.out.println("max: "+getMax)
	System.out.println("min: "+getMin)
	System.out.println("total: "+normalsum);
	
	var last=Double.NegativeInfinity
    for(b <- bins ){
     val num=(b._2.toDouble/normalsum)*100;	
	 System.out.print("["+("%.4f".format(last)).substring(0,6)+","+("%.4f".format(b._1))+"]     ");
	 for( i <- Range(0,num.toInt) ){
	  System.out.print("|");
	 }//
	 last=b._1;
	 System.out.println("    ("+b._2+") \n");
    }//for
  }
}
object Histogram {
  def fromString(str:String) = {
    val values = str.split(";")
    val feature=values(0).toInt
    val maxBins=values(1).toInt
    val bins = values(2).split(",").map( x=> (x.split(" ")(0).toDouble, x.split(" ")(1).toInt ) )
    val h = new Histogram(feature,maxBins)
    bins.foreach( b => h.update(b._1, b._2) )
    h
  }
}