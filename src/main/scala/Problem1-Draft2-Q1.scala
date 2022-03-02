
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.runtime.universe._


object ToolBoxDraft2 extends Serializable {
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




object Problem1SolutionDraft2 extends Serializable {


  def getPeopleByBin(sc: SparkContext, peopleFile : String, infectedFile : String ) = {
    val uninfectedPeopleByBin = sc.textFile(peopleFile)
      .map { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        (ToolBoxDraft2.findBinNumber(x, y), ((x, y), false, pid))
      }.groupByKey()

    val infectedPeopleByBin = sc.textFile(infectedFile)
      .flatMap { tuple =>
        val splitTuple = tuple.split(",")
        val pid = splitTuple(0).toInt
        val x = splitTuple(1).toDouble
        val y = splitTuple(2).toDouble
        ToolBoxDraft2.getAllInfectedBins(x, y, pid)
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
              if (!uninfectedPeopleSet.contains(uninfectedPerson._3) && ToolBoxDraft2.getDistance(infectedPerson._1, uninfectedPerson._1) < 7) {
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