import scala.collection.mutable.ListBuffer

object ToolBox extends Serializable {
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
