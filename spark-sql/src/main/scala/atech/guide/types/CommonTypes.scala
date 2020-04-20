package atech.guide.types

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object CommonTypes extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  /**
    * Adding a value to a DF
    */
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  /**
    * Boolean
    */
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select(col("Title")).where(dramaFilter)

  // Evaluating a filter as value in Dataframe
  val moviesWithGoodnessFlags = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  /**
    * Filter on a boolean Column
    * +--------------------------------+----------+
    * |Title                           |good_movie|
    * +--------------------------------+----------+
    * |12 Angry Men                    |true      |
    * |Twelve Monkeys                  |true      |
    * |Twin Falls Idaho                |true      |
    * |Amen                            |true      |
    */
  moviesWithGoodnessFlags.where("good_movie") // where(col("good_movie") === "true")

  // Negations
  moviesWithGoodnessFlags.where(not(col("good_movie")))

  /**
    * Numbers
    */

  // Math Operators
  val moviesAverageRating = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an Action */)

  /**
    * Strings
    */

  private val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkawagen|vw"
  val vwDF = carsDF
    .select(col("Name"), regexp_extract(col("Name"), regexString, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    *   - contains
    *   - regexes
    */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // Solution 1
  val complexRegex = getCarNames.map(_.toLowerCase).mkString("|") // volskwagen|mercedes-benz|ford

  carsDF
    .select(col("Name"), regexp_extract(col("Name"), complexRegex, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "")

  // Solution 2
  val carNameFilters: List[Column] = getCarNames.map(_.toLowerCase).map(name => col("Name").contains(name))
  val bigFilter: Column = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)

  println(bigFilter) // (((false OR contains(Name, volkswagen)) OR contains(Name, mercedes-benz)) OR contains(Name, ford))

  carsDF.filter(bigFilter).show()















}
