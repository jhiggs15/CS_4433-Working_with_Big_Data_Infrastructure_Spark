
import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkContext}

import scala.collection.mutable


object Problem1SolutionDraft4 extends Serializable {
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

  def Q2(sc: SparkContext, peopleFile : String, infectedLargeFile : String, outputFile : String): Unit = {
    val allPeopleByBin = getPeopleByBin(sc, peopleFile, infectedLargeFile)

    val atRiskPeople = allPeopleByBin
      .flatMap { bin =>
        val people: Iterable[((Double, Double), Boolean, Int)] = bin._2
        val numOfInfectedContacts = new ListBuffer[(Int, Int)]()

        val infectedPeople = people.filter(value => value._2)
        val uninfectedPeople = people.filter(value => !value._2)

        for (infectedPerson <- infectedPeople) {
          numOfInfectedContacts.append((infectedPerson._3, 0))

          for (uninfectedPerson <- uninfectedPeople) {
            if (ToolBox.getDistance(infectedPerson._1, uninfectedPerson._1) < 6) {
              numOfInfectedContacts.append((infectedPerson._3, 1))
            }
          }
        }
        numOfInfectedContacts
      }.reduceByKey(_ + _).saveAsTextFile(outputFile + "2")
  }







}




//  }