package in.kamranali.superhero

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularSuperhero {

  def parseNames(line: String): Option[(Int, String)] = {

    // Splitting on while spaces
    var fields = line.split('\"')

    if (fields.length > 1) {
      return Some(fields(0).trim.toInt, fields(1))
    } else {
      // Flatmap will discard data from None results and extract data from some results.
      return None
    }

  }

  def countCoOccurences(line: String)= {

    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularSuperheroApp")

    // Contains Super hero Name -> ID data
    val names = sc.textFile("./src/main/resources/superherodata/Marvel-names.txt")

    val namesRDD = names.flatMap(parseNames)

    val lines = sc.textFile("./src/main/resources/superherodata/Marvel-graph.txt")

    // It has HeroID -> No of connections
    val pairing = lines.map(countCoOccurences)

    //Combining more than one line for same superHero ID
    val totalFriendsByCharacter = pairing.reduceByKey((x,y) => x + y)

    // Flipping it to No of connection -> HeroID
    val flipped = totalFriendsByCharacter.map(x => (x._2, x._1))

    val mostPopular = flipped.max()

    val mostPopularName = namesRDD.lookup(mostPopular._2)(0)

    print(s"$mostPopularName is the most popular super hero with ${mostPopular._1} co-appearances")


  }

}
