
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.runtime.universe._


object Problem1SolutionDraft3 extends Serializable {
  // file specifies using local files, not those on hdfs
  // for hdfs do not use file://


  def getPeopleByBin(sc: SparkContext, peopleFile : String, infectedFile : String ) = {
    val uninfectedPeopleByBin = sc.textFile(peopleFile)
      .map { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        (ToolBox.findBinNumber(x, y), ((x, y), false, pid))
      }

    val infectedPeopleByBin = sc.textFile(infectedFile)
      .flatMap { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        ToolBox.getAllInfectedBins(x, y, pid)
      }

    uninfectedPeopleByBin.union(infectedPeopleByBin).groupByKey()

  }

  def Q1(sc: SparkContext, peopleFile : String, infectedSmallFile : String, outputFile : String): Unit = {
    // read in data
    //    "s".toInt

    val allPeopleByBin = getPeopleByBin(sc, peopleFile, infectedSmallFile)

    val atRiskPeople = allPeopleByBin
      .flatMap { bin =>
        val people: Iterable[((Double, Double), Boolean, Int)] = bin._2
        val uniquePeople = new ListBuffer[Int]()

        // if there are two lists the first is uninfected people, the second is infected people
        val infectedPeople = people.filter(value => value._2)
        val uninfectedPeople = people.filter(value => !value._2)

        for (infectedPerson <- infectedPeople) {
          for (uninfectedPerson <- uninfectedPeople) {
            if (ToolBox.getDistance(infectedPerson._1, uninfectedPerson._1) < 6) {
              uniquePeople.append(uninfectedPerson._3)
            }
          }
        }

        uniquePeople


      }.distinct().saveAsTextFile(outputFile + "1")

  }







}




//  }