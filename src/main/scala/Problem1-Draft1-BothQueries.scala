
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.universe._


object Problem1SolutionDraft extends Serializable {
  // file specifies using local files, not those on hdfs
  // for hdfs do not use file://
  //  val peopleFile = "file:///home/ds503/shared_folder/People.csv"
  val peopleFile = "file:///home/ds503/shared_folder/People-Test-Copy.csv"
  val infectedLargeFile = "file:///home/ds503/shared_folder/Infected-Large.csv"
  //  val infectedSmallFile = "file:///home/ds503/shared_folder/Infected-Small.csv"
  val infectedSmallFile = "file:///home/ds503/shared_folder/Infected-Test-Small-Copy.csv"
  val outputFile = "file:///home/ds503/shared_folder/D2Problem1Q"

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
          val uninfectedPeople = people.head
          val infectedPeople = people.last

          for (infectedPerson <- infectedPeople) {
            for (uninfectedPerson <- uninfectedPeople) {
              if (ToolBox.getDistance(infectedPerson._1, uninfectedPerson._1) < 6) {
                uniquePeople.append(uninfectedPerson._3)
              }
            }
          }

        }
        uniquePeople


      }.distinct().saveAsTextFile(outputFile + "1")

  }

  def Q2(sc: SparkContext, peopleFile : String, infectedLargeFile : String, outputFile : String): Unit = {
    val allPeopleByBin = getPeopleByBin(sc, peopleFile, infectedLargeFile)

    val atRiskPeople = allPeopleByBin
      .flatMap { bin =>
        val people = bin._2
        val numOfInfectedContacts = new ListBuffer[(Int, Int)]()

        // if there are two lists the first is uninfected people, the second is infected people
        if(people.size == 2) {
          val uninfectedPeople = people.head
          val infectedPeople = people.last

          for (infectedPerson <- infectedPeople) {
            numOfInfectedContacts.append((infectedPerson._3, 0))

            for (uninfectedPerson <- uninfectedPeople) {
              if (ToolBox.getDistance(infectedPerson._1, uninfectedPerson._1) < 6) {
                numOfInfectedContacts.append((infectedPerson._3, 1))
              }
            }
          }
        }
        numOfInfectedContacts
      }.reduceByKey(_ + _).saveAsTextFile(outputFile + "2")
  }




}




//  }