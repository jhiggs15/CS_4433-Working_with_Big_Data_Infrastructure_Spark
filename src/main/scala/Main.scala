import org.apache.spark.{SparkConf, SparkContext}

object Main {
  // file specifies using local files, not those on hdfs
  // for hdfs do not use file://
  val peopleFile = "file:///home/ds503/shared_folder/People.csv"
//  val peopleFile = "file:///home/ds503/shared_folder/People-Test-Copy.csv"
  val infectedLargeFile = "file:///home/ds503/shared_folder/Infected-Large.csv"
  val infectedSmallFile = "file:///home/ds503/shared_folder/Infected-Small.csv"
//  val infectedSmallFile = "file:///home/ds503/shared_folder/Infected-Test-Small-Copy.csv"
  val outputFile = "file:///home/ds503/shared_folder/Problem1Q"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TransformationOperator")
    val sc: SparkContext = new SparkContext(conf)
    var startTime : Long = 0;
    var endTime : Double = 0;

    if (args.length == 4) {
      if (args(0) == "1") {
        startTime = System.currentTimeMillis()
        Problem1Solution.Q1(sc, args(1), args(2), args(3))
        endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
      }
      else if (args(0) == "2") {
        startTime = System.currentTimeMillis()
        Problem1Solution.Q2(sc, args(1), args(2), args(3))
        endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
      }
    }
    else if (args.length == 5) {
      if(args(1) == "1") {
        args(0) match {
          case "final" => {
            startTime = System.currentTimeMillis()
            Problem1Solution.Q1(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case "firstDraft" => {
            startTime = System.currentTimeMillis()
            Problem1SolutionDraft.Q1(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case "secondDraft" => {
            startTime = System.currentTimeMillis()
            Problem1SolutionDraft2.Q1(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case "thirdDraft" => {
            startTime = System.currentTimeMillis()
            Problem1SolutionDraft3.Q1(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case _ => {
            startTime = System.currentTimeMillis()
            Problem1Solution.Q1(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
        }
      }
      else if (args(1) == "2") {
        args(0) match {
          case "final" => {
            startTime = System.currentTimeMillis()
            Problem1Solution.Q2(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case "firstDraft" => {
            startTime = System.currentTimeMillis()
            startTime = System.currentTimeMillis()
            Problem1SolutionDraft.Q2(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case "secondDraft" => {
            startTime = System.currentTimeMillis()
            Problem1SolutionDraft4.Q2(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
          case _ => {
            startTime = System.currentTimeMillis()
            Problem1Solution.Q2(sc, args(2), args(3), args(4))
            endTime = ((System.currentTimeMillis() - startTime) / 1000.0)
          }
        }
      }

    }

    println("Problem " + args(0) +  " Time " + endTime + " seconds")







  }
}
