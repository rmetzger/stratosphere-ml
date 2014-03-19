package bigdataproject.scala.eu.stratosphere.ml.randomforest


object RandomForestExecutor {
 
  def main(args: Array[String]) {
    if(args.length < 3){
      println("not enough parameters")
      println("[build number-trees¦eval] dest-path data-source  [remoteJar remoteHost remotePort]")
      return
    }
    val mode=args(0)
    for( i <- args ){
      System.out.println(i)
    }

    if(mode == "build"){
      val strategy=args(1)
      val path=args(2)
      val data=args(3)
      val trees=args(4).toInt

      var remoteJar : String =null
      var remoteHost : String =null
      var remotePort : Int =0

      if(args.length > 5)
        remoteJar=args(5)
      if(args.length > 6)
        remoteHost=args(6)
      if(args.length > 7)
        remotePort=args(7).toInt

    new RandomForestBuilder(remoteJar,remoteHost,remotePort).build(
        path,
        data,
        trees,
        strategy
        )
    } else  if(mode == "eval"){
      val path=args(1)
      val data=args(2)

      var remoteJar : String =null
      var remoteHost : String =null
      var remotePort : Int =0

      if(args.length > 3)
        remoteJar=args(3)
      if(args.length > 4)
        remoteHost=args(4)
      if(args.length > 5)
        remotePort=args(5).toInt

      new RandomForestBuilder().eval(
        data,
        path+"rf_output_tree",
        path+"rf_output_evaluation"
      )
    } else {
      print("unknown mode")
    }
    System.exit(0)
  }
 
}