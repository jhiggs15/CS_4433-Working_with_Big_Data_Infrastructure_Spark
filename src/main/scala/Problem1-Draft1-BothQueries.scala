
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.universe._

object ToolBoxDraft extends Serializable {
  val binSize = 6.0
  val numberofRows = 10000.0

  def findBinNumber(x : Double, y : Double) : Double = {

    val binsPerRow = math.ceil(numberofRows / binSize)

    val binColNumber = truncate(x / binSize) + 1
    val binRowNumber = truncate(y / binSize)

    binColNumber + (binRowNumber * binsPerRow)

  }

  def truncate(value : Double) : Long = {
    math.round(value - .5)
  }

  // bins in all eight directions are touched
  def getAllInfectedBins(x : Double, y : Double, pid : Int) : ListBuffer[(Double, ((Double, Double) , Boolean, Int))] = {
    var infectedBins = ListBuffer((findBinNumber(x, y), ((x, y), true, pid)))



    if (x - binSize >= 0) {
      infectedBins.append((findBinNumber(x - binSize, y), ((x, y), true, pid)))
      infectedBins.append((findBinNumber(x - binSize, y + binSize), ((x, y), true, pid)))
    }
    if (y - binSize >= 0) {
      infectedBins.append((findBinNumber(x, y - binSize), ((x, y), true, pid)))
      infectedBins.append((findBinNumber(x + binSize, y - binSize), ((x, y), true, pid)))
    }

    if (x - binSize >= 0 && y - binSize >= 0) {
      infectedBins.append((findBinNumber(x - binSize, y - binSize), ((x, y), true, pid)))
    }

    infectedBins.append((findBinNumber(x + binSize, y), ((x, y), true, pid)))
    infectedBins.append((findBinNumber(x, y + binSize), ((x, y), true, pid)))
    infectedBins.append((findBinNumber(x + binSize, y + (binSize)), ((x, y), true, pid)))

    infectedBins


  }

  def getDistance(coord1 : (Double, Double), coord2 : (Double, Double)) : Double = {
    math.abs(math.sqrt(math.pow(coord2._1 - coord1._1, 2) + math.pow(coord2._2 - coord1._2, 2)))
  }

  // adapted from https://squidarth.com/scala/types/2019/01/11/type-erasure-scala.html
  def paramInfo[T: TypeTag](x: T): Boolean = {
    typeOf[T] match {
      case t if t =:= typeOf[Iterable[Iterable[((Double, Double), Boolean, Int)]]] => true
      case _ => false
    }
  }

}




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
        (ToolBoxDraft.findBinNumber(x, y), ((x, y), false, pid))
      }.groupByKey()

    val infectedPeopleByBin = sc.textFile(infectedFile)
      .flatMap { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        ToolBoxDraft.getAllInfectedBins(x, y, pid)
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
              if (ToolBoxDraft.getDistance(infectedPerson._1, uninfectedPerson._1) < 7) {
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
              if (ToolBoxDraft.getDistance(infectedPerson._1, uninfectedPerson._1) < 7) {
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