package atech.guide.concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val genreCount1 = moviesDF.select(count("Major_Genre")) // All values except null
  val genreCount2 = moviesDF.selectExpr("count(Major_Genre)")

  // Counting All
  val countAllRows = moviesDF.select(count("*")) // Count all ROWS and will INCLUDE nulls

  // Counting Distinct
  val genreCountDistinct = moviesDF.select(countDistinct("Major_Genre"))

  // Approximate count
  val genreCountDistinctApprox = moviesDF.select(approx_count_distinct("Major_Genre")) // Will not scan entire DF but rather will give an approx row count

  // Min, Max
  val minRating = moviesDF.select(min("IMDB_Rating"))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // Average
  moviesDF.select(avg("Rotten_Tomatoes_Rating"))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean("Rotten_Tomatoes_Rating"),
    stddev("Rotten_Tomatoes_Rating")
  )

  // Grouping
  // select count(*) from moviesDF group by Major_Genre
  val countByGenre = moviesDF.groupBy("Major_Genre") // includes null
    .count()

  val averageRAtingByGenre = moviesDF.groupBy("Major_Genre")
    .avg("IMDB_Rating")

  moviesDF.groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")

  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */


  /**
    * +--------------------------------------+
    * |(sum(US_Gross) + sum(Worldwide_Gross))|
    * +--------------------------------------+
    * |                          413129480065|
    * +--------------------------------------+
    */
  moviesDF.select(sum("US_Gross") + sum("Worldwide_Gross"))

  /**
    * +----------------+
    * |sum(Total_Gross)|
    * +----------------+
    * |    413129480065|
    * +----------------+
    */
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")) // + col("US_DVD_Sales")
    .select(sum("Total_Gross"))

  /**
    * +------------------------------------------------------------+
    * |((sum(US_Gross) + sum(Worldwide_Gross)) + sum(US_DVD_Sales))|
    * +------------------------------------------------------------+
    * |                                                432813952470|
    * +------------------------------------------------------------+
    */
  moviesDF.select(sum("US_Gross") + sum("Worldwide_Gross") + sum("US_DVD_Sales"))

  /**
    * +----------------+
    * |sum(Total_Gross)|
    * +----------------+
    * |    139190135783|
    * +----------------+
    */
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")) //
    .select(sum("Total_Gross"))


  // 2
  moviesDF.select(countDistinct("Director"))

  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  // 4
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
