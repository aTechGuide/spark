package atech.guide.datasets

import java.sql.Date

import org.apache.spark.sql.{Dataset, Encoder, Encoders, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

/**
  * Spark allows us to add more Type information to this dataframe turning it into a dataset.
  * Then it will allow us to process this dataset with predicate of our choice as it were a distributed collection of JVM object
  */
object DatasetBasics extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val numberssDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/numbers.csv")


  numberssDF.printSchema()

  // Encoder[Int] turns a Row of dataframe into an Int
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numberssDF.as[Int]

  numbersDS.filter(_ < 100)

  /**
    *
    * Dataset of a complex Type
    */
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")
    .withColumn("Year", col("Year").cast(DateType))

  // All case classes extends Product Type. This helps spark to identify which fields map to each column in a dataframe
  implicit val carEncoder: Encoder[Car] = Encoders.product[Car]
  val carsDS = carsDF.as[Car]

  // DS Collection functions
  numbersDS.filter(_ < 100)

  import spark.implicits._ // Imports all Encoders that we will ever need

  // map, flatMap, fold, reduce, filter ...
  carsDS.map(car => car.Name.toUpperCase())

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  // 1
  val countCars = carsDS.count

  // 2
  carsDS.filter( _.Horsepower.getOrElse(0L) > 140).count

  // 3
  carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / countCars

  // Also use DF functions
  carsDS.select(avg(col("Horsepower")))

  /**
    * Joins
    */

  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
    * +--------------------+--------------------+
    * |                  _1|                  _2|
    * +--------------------+--------------------+
    * |[1, [1], 1, Angus...|[Sydney, 1, AC/DC...|
    * |[0, [0], 0, Jimmy...|[London, 0, Led Z...|
    * |[3, [3], 3, Kirk ...|[Los Angeles, 3, ...|
    * +--------------------+--------------------+
    */
  // guitarPlayerBandsDS.show()

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    //.show()

  /**
    * Grouping DS
    */

  val carsGroupedByOrigin: KeyValueGroupedDataset[String, Car] = carsDS.groupByKey(_.Origin)
  carsGroupedByOrigin.count()
    //.show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations


}
