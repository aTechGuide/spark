package atech.guide.datasources

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object ReadWriteDF extends App {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading DF
    - format
    - schema (optional) or inferSchema = true
    - path
    - zero or more options
   */
  private val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // <- Decides what spark should do in case it encounters a malformed record. Other options: dropMalformed, permissive (default)
    .load("src/main/resources/data/cars.json")


  private val carsDFWithOptionMap = spark.read
    .format("json")
    .schema(carsSchema)
    .options(Map("mode" -> "failFast"))
    .load("src/main/resources/data/cars.json")

  /*
    Writing DF
    - Setting a format
    - Save mode: override, append, ignore, errorIfExists
    - path
    - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("output_car_json")


  /**
    * JSON Flags
    */
  spark.read
    .schema(carsSchema)
    .option("dataFormat", "YYYY-MM-dd") // <- dataFormat only works with enforced schema otherwise spark does not have  a clue which field to parse for date. If spark fails parsing it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")// other values: bzip2, gzio, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")


  /**
    * CSV flags
    */
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dataFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",") //<- SEparator for values
    .option("nullValue", "") // <- Instructs spark to parse "" as null in resulting DF
    .csv("src/main/resources/data/stocks.csv")

  /**
    * Parquet
    * - open source
    * - compressed
    * - binary data storage format
    * - optimized for fast reading of columns
    *
    * - Default storage format for dataframes
    */

  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("output_car") //<- OR we can say, .save("output") as parquest is default storage format


  /**
    * Text files
    */
  spark.read
    .text("src/main/resources/data/sample.txt")

  /**
    * Reading from Postgres DB
    */

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/atechguide")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise
    */

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  // Tab separated csv file
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("output_movie_csv")

  // Saving in parquet
  moviesDF.write.save("output_movie_parquet")

  // Saving to DB
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/atechguide")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()




}
