package in.kamranali.basics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/*
Find sorted count for each rating.
Given Data format (userID, movieID, rating, timestamp)
 */
object RatingsCounter {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create Spark context using every core of local machine
    // Remember: SparkContext object is used to create our RDDs
    val sc = new SparkContext("local[*]", "RatingCounterApp")

    // Read the data file
    // Loading data into RDD constant lines
    val lines = sc.textFile("./src/main/resources/moviedata/u.data")

    // Convert each line to String -> Split by Tabs -> Extract third field
    // Data format (userID, movieID, rating, timestamp)
    // Transforming lines RDD to rating RDD
    val rating = lines.map(x => x.toString.split("\t")(2))

    // Count number of times each rating appears
    // countByValue takes an RDD and returns scala map object
    // countByValue() is an action which means DAG is created in this step
    val results = rating.countByValue()

    // Sort the resulting map of (rating, count) tuples
    // Sorting by first value of Sequence
    val sortedResult = results.toSeq.sortBy(_._1)

    // Print each result
    sortedResult.foreach(println)
  }
}
