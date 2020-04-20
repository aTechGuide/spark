package atech.guide.types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ArraysBasics extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesWithWords = moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // Array of Strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), //<- Title_Words[0] means first word
    size(col("Title_Words")), // Number of words in the array
    array_contains(col("Title_Words"), "Love")
  ).show()



}
