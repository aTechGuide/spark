package atech.guide.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext //<- Entry point for creating RDDs

  // 1 - Parallelize existing Collection
  val numbers = 1 to 1000000
  val numbersRDD: RDD[Int] = sc.parallelize(numbers)

  // 2a - Reading from files
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(fileName: String) =
    Source.fromFile(fileName)
    .getLines()
    .drop(1)
    .map(line => line.split(","))
    .map((tokens: Array[String]) => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    .toList


  val stocksRDD1 = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - Reading from files

  val stocksRDD2 = sc
    .textFile("src/main/resources/data/stocks.csv") // Returns RDD of strings of rows
    // .drop(1) //<- We can NOT do this. As RDDs are distributed collentions so we do not know which entery we are dropping from the RDD
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0))
    .map((tokens: Array[String]) => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 Read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksRDD3: RDD[Row] = stocksDF.rdd

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD4: RDD[StockValue] = stocksDS.rdd

  // RDD to DF [We lose Type Information]
  val numbersDF: Dataset[Row] = numbersRDD.toDF("numbers")

  // RDD to DS [We Keep Type Information]
  val numbersDS1: Dataset[Int] = numbersRDD.toDS()
  val numbersDS2: Dataset[Int] = spark.createDataset(numbersRDD)

  /**
    * Transformations
    */

  // count
  val msftRDD = stocksRDD1.filter(_.symbol == "MSFT") // lazy transformation
  val msftCount = msftRDD.count() // eager Action

  // distinct
  val companyNamesRDD = stocksRDD1.map(_.symbol).distinct()

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)

  val minMSFT = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping [Expensive -> Shuffling]
  val groupedStocksRDD = stocksRDD1.groupBy(_.symbol)

  // Partitioning

  /*
     Repartitioning is EXPENSIVE. Involves Shuffling.
     Best practice: partition EARLY, then process that.
     Size of a partition 10-100MB.
  */
  val repartitionedStockRDD = stocksRDD1.repartition(30)

  // coalesce
  val coalescedRDD = repartitionedStockRDD.coalesce(15)
  // ^^ does NOT involve shuffling. As 15 partitions gonna stay in same place and other partitions are going to move data to them. So not a true shuffling in between cluster.

  /**
    * Exercises
    *
    * 1. Read the movies.json as an RDD.
    * 2. Show the distinct genres as an RDD.
    * 3. Select all the movies in the Drama genre with IMDB rating > 6.
    * 4. Show the average rating of movies by genre.
    */

  case class Movie(title: String, genre: String, rating: Double)

  val moviesDF = spark.read
    .option("interSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

//  moviesRDD.toDF.show

  // 2
  val genreRDD = moviesRDD.map(_.genre).distinct
//  genreRDD.toDF.show

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
//  goodDramasRDD.toDF.show

  // 4
  case class GenreAverageRating(genre: String, rating: Double)

  val avgRatingByGenreGroup:RDD[(String, Iterable[Movie])] = moviesRDD.groupBy(_.genre)

  val avgRatingByGenreRDD = avgRatingByGenreGroup.map {
    case (genre, movies) => GenreAverageRating(genre, movies.map(_.rating).sum / movies.size)

  }

  avgRatingByGenreRDD.toDF.show

  // By Regular DF
  moviesRDD.toDF
    .groupBy(col("genre"))
    .agg(avg("rating"))
    .show()

}
