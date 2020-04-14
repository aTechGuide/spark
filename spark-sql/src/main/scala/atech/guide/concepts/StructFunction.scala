package atech.guide.concepts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/**
 * Reference -> https://sparkbyexamples.com/spark/spark-sql-structtype-on-dataframe/
 */
object StructFunction {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Option 2 [Create a schema manually]
  private val carsSchema = StructType(Array(
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
  private val data = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  def main(args: Array[String]): Unit = {

    data.limit(5).show(false)

    val dataWithStruct = data
      .select(col("Name"), col("Miles_per_Gallon"), struct("Cylinders", "Displacement", "Horsepower").as("metadata"))

    /**
     * root
     * |-- Name: string (nullable = true)
     * |-- Miles_per_Gallon: double (nullable = true)
     * |-- metadata: struct (nullable = false)
     * |    |-- Cylinders: long (nullable = true)
     * |    |-- Displacement: double (nullable = true)
     * |    |-- Horsepower: long (nullable = true)
     */
    dataWithStruct.printSchema

    /**
     * +-------------------------+----------------+---------------+
     * |Name                     |Miles_per_Gallon|metadata       |
     * +-------------------------+----------------+---------------+
     * |chevrolet chevelle malibu|18.0            |[8, 307.0, 130]|
     * |buick skylark 320        |15.0            |[8, 350.0, 165]|
     * +-------------------------+----------------+---------------+
     */
    dataWithStruct.show(2, truncate = false)

  }

}
