package atech.guide.concepts

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

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


  private val carsDF = spark.read
    .schema(carsSchema)
    .option("mode", "dropMalformed")
    .json("src/main/resources/data/cars.json")

  /**
    * Columns
    * - They are special JVM objects that will allow us to obtain new Dataframe out of source dataframes by processing the data inside
    * - They have no data inside
    */

  val firstColumn: Column = carsDF.col("Name")

  // Selection (Technically known as Projection)
  val carNamesDF = carsDF.select(firstColumn) // We obtainf new DF from carsDF as DF is immutable
//  carNamesDF.show()

  // various select methods
  carsDF.select(
    carsDF.col("Name"),
    carsDF.col("Acceleration")
  )

  // Using col method from
  // col("Name") does the same thing as carsDF.col("Name") but there is a small difference between them. [In Joins we will discuss it]

  import spark.implicits._
  carsDF.select(
    col("Name"),
    column("Acceleration"), // col() and column() does exact same thing
    'Year, // Scala Symbol, which is auto converted to column
    $"Horsepower", // fancier interpolated string returns a column object
    expr("Origin") // Expression
  )

  // Passing Strings to select column
  carsDF.select("Name", "Year")

  /**
    * Expression
    */
  val simplestExpression: Column = carsDF.col("Weight_in_lbs") // Column object is a sub type of Expression
  val weightInKGExpression: Column = carsDF.col("Weight_in_lbs") / 2.2 // This describes a transformation

  val carsWithWeight = carsDF.select(
    col("Name"),
    simplestExpression,
    weightInKGExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

//  carsWithWeight.show()

  // select + expr
  val carsWithSelectExpr = carsDF
    .selectExpr("Name", "Weight_in_lbs", "Weight_in_lbs / 2")

  /**
    * DF Processing
    */

  // Adding a new column
  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // Renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds") // Careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`") // Use backticks when we have hyphens or spaces

  // Remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  /**
    * Filtering
    */

  val euCArsDF1 = carsDF.filter(col("origin") =!= "USA")
  val euCArsDF2 = carsDF.where(col("origin") =!= "USA")

  // filtering with expression strings
  val americanCarDF = carsDF.filter("Origin = 'USA'")

  // Chain filters
  val americanPowerfulCarsDF1 = carsDF.filter(col("origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("origin") === "USA" and col("Horsepower") > 150) // Combining above filter by AND operator (AND is infixed)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  /**
    * Unioning = Adding more Rows
    */

  val moreCarsDF = spark.read
    .schema(carsSchema)
    .json("src/main/resources/data/more_cars.json")


  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  /**
    * Distinct Values
    */
  val allCountriesDF = carsDF.select("Origin").distinct()
//  allCountriesDF.show()

  /**
    * Exercises
    *
    * 1. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 2. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val totalProfit1 = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_SAles"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val totalProfit2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_SAles",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  val totalProfit3 = moviesDF.select("Title","US_Gross", "Worldwide_Gross")
      .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 2
  val comedy = moviesDF.select("Title","IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  comedy.show()




}
