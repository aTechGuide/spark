package in.kamranali.superhero

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkContext}

import scala.collection.mutable.ArrayBuffer

/*Find degree of separation between two comic book characters*/
object DegreeOfSeparation {

  val startCharaterID = 5306 // Spiderman
  val targetCharaterID = 5306 // ADAM, 3,031

  var hitCounter:Option[Accumulator[Int]] = None

  // type is a custom datatype in scala e.g. struct of C++
  // BFSData contains array of HeroID connections, the distance and color
  type BFSData = (Array[Int], Int, String)

  // BFSNode has heroID and BFSData associated with it
  type BFSNode = (Int, BFSData)

  def convertToBFSNode(line: String): BFSNode = {

    val fields = line.split("\\s+")
    val heroID = fields(0).toInt

    // Extract connected heroIDs into connections Array

    var connections: ArrayBuffer[Int] = ArrayBuffer()

    for (connection <- 1 to (fields.length - 1)) {
      connections += fields(connection).toInt
    }

    var color:String = "WHITE"
    var distance:Int = 9999

    if (heroID == startCharaterID) {
      color = "GRAY"
      distance = 0
    }

    return (heroID, (connections.toArray, distance, color))
  }

  def createStartingRDD(sc: SparkContext) : RDD[BFSNode] = {

    // Returs Array of BFSNode
    val inputFile = sc.textFile("./src/main/resources/superherodata/Marvel-graph.txt")
    return inputFile.map(convertToBFSNode)

  }

  /*Expands a BFSNode into this node and its children*/
  def bfsMap(node: BFSNode): Array[BFSNode] = {

    val characterID:Int = node._1
    val data:BFSData = node._2

    var connections:Array[Int] = data._1
    var distance:Int = data._2
    var color:String = data._3

    // We return Array of many BFSNodes to add to our new RDD
    var results:ArrayBuffer[BFSNode] = ArrayBuffer()

    // Gray nodes are flagged for expansion, create new Gray nodes for each connection
    if(color == "GRAY") {

      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"


        if (targetCharaterID == connection) {
          if(hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        val newEntry:BFSNode = (newCharacterID , (Array(), newDistance, newColor))
        results += newEntry
      }

      color = "BLACK"
    }

    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry

    return results.toArray
  }


  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1:BFSData, data2:BFSData): BFSData = {

    // Extract data that we are combining
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3

    // Default node values
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }

    return (edges.toArray, distance, color)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "DegreeOfSeparationeApp")

    hitCounter = Some(sc.accumulator(0))

    var iterationRDD = createStartingRDD(sc)

    // 10 => Arbitary iteration that we will do to figure out who is actually connected in the social Graph

    for (iteration <- 1 to 10) {
      print("Running BFS iteration # ", iteration)

      val mapped = iterationRDD.flatMap(bfsMap)

      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          print("Hit the target character! From " + hitCount + " different direction(s).")

          return
        }
      }

      // It combines data for each CharacterID, preserving the daarkest color and shortest path

      iterationRDD = mapped.reduceByKey(bfsReduce)

    }
  }


}
