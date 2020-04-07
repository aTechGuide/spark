package atech.guide.dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/**
 * Dataframe -> Distributed collection of rows confirming to a schema
 *
 * - Schema
 */
object DataframeBasics extends App {

  val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  val dataCar1 = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // data.show()

  /**
    schema
   */

  // Option 1 [Obtain a schema]
  val carsSchemaFromDF = dataCar1.schema

  // Option 2 [Create a schema manually]
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // Read Dataframe with Schema
  val dataCar2 = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  // Create rows by hand
  val myRow = Row("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA")

  // Create DF from Tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  /*
    Note:
    - Schema is only applicable to Data frame not to rows
    - Rows = Unstructured data

    Spark Types:
    - Are known to spark at run time rather than compile type
   */

  // Create DFs from implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF()


}
