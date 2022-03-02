
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.runtime.universe._






object Problem1Solution extends Serializable {


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
        //        val uniquePeople = new ListBuffer[Int]()

        // if there are two lists the first is uninfected people, the second is infected people
        val uninfectedPeopleSet: mutable.Set[Int] = new mutable.HashSet()
        val infectedPeople = people.filter(value => value._2)
        val uninfectedPeople = people.filter(value => !value._2)

        for (infectedPerson <- infectedPeople) {
          for (uninfectedPerson <- uninfectedPeople) {
            if (!uninfectedPeopleSet.contains(uninfectedPerson._3) && ToolBox.getDistance(infectedPerson._1, uninfectedPerson._1) < 7) {
              uninfectedPeopleSet.add(uninfectedPerson._3)
            }
          }
        }

        uninfectedPeopleSet


      }.saveAsTextFile(outputFile + "1")

  }



  def Q2(sc: SparkContext, peopleFile : String, infectedLargeFile : String, outputFile : String): Unit = {
    val allPeopleByBin = getPeopleByBin(sc, peopleFile, infectedLargeFile)

    val atRiskPeople = allPeopleByBin
      .flatMap { bin =>
        val people: Iterable[((Double, Double), Boolean, Int)] = bin._2
        //          val numOfInfectedContacts = new ListBuffer[(Int, Int)]()
        val numOfInfectedContacts: mutable.Map[Int, Int] = new mutable.HashMap()


        val infectedPeople = people.filter(value => value._2)
        val uninfectedPeople = people.filter(value => !value._2)

        for (infectedPerson <- infectedPeople) {
          numOfInfectedContacts.put(infectedPerson._3, 0)

          for (uninfectedPerson <- uninfectedPeople) {
            if (ToolBoxDraft.getDistance(infectedPerson._1, uninfectedPerson._1) < 7) {
              val oldNumber : Int = numOfInfectedContacts.get(infectedPerson._3).get
              numOfInfectedContacts.put(infectedPerson._3, oldNumber + 1)
            }
          }
        }
        numOfInfectedContacts
      }.reduceByKey(_ + _).saveAsTextFile(outputFile + "2")
  }



}




//  }