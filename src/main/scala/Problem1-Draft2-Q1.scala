
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.runtime.universe._



object Problem1SolutionDraft2 extends Serializable {


  def getPeopleByBin(sc: SparkContext, peopleFile : String, infectedFile : String ) = {
    val uninfectedPeopleByBin = sc.textFile(peopleFile)
      .map { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        (ToolBox.findBinNumber(x, y), ((x, y), false, pid))
      }.groupByKey()

    val infectedPeopleByBin = sc.textFile(infectedFile)
      .flatMap { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        ToolBox.getAllInfectedBins(x, y, pid)
      }.groupByKey()

    uninfectedPeopleByBin.union(infectedPeopleByBin).groupByKey()

  }


  def Q1(sc: SparkContext, peopleFile : String, infectedSmallFile : String, outputFile : String): Unit = {
    // read in data
    //    "s".toInt

    val allPeopleByBin = getPeopleByBin(sc, peopleFile, infectedSmallFile)

    val atRiskPeople = allPeopleByBin
      .flatMap { bin =>
        val people = bin._2
        val uniquePeople = new ListBuffer[Int]()

        // if there are two lists the first is uninfected people, the second is infected people
        if (people.size == 2) {
          var uninfectedPeopleSet: mutable.Set[Int] = new mutable.HashSet()
          val infectedPeople = people.last
          val uninfectedPeople = people.head

          for (infectedPerson <- infectedPeople) {
            for (uninfectedPerson <- uninfectedPeople) {
              if (!uninfectedPeopleSet.contains(uninfectedPerson._3) && ToolBox.getDistance(infectedPerson._1, uninfectedPerson._1) < 6) {
                uniquePeople.append(uninfectedPerson._3)
                uninfectedPeopleSet.add(uninfectedPerson._3)
              }
            }
          }

        }
        uniquePeople


      }.saveAsTextFile(outputFile + "1")

  }

}




//  }