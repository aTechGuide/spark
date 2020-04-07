package atech.guide.concepts

import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._


/**
 * Ref: https://sparkbyexamples.com/spark/explode-spark-array-and-map-dataframe-column/
 *
 * Explode creates Array / Map column to Rows
 */
object Explode {

  private val spark = SparkSession
    .builder
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val data = Seq(
      Row("James", List("Java","Scala"), Map("hair"->"black","eye"->"brown")),
      Row("Michael", List("Spark","Java",null), Map("hair"->"brown","eye"->null)),
      Row("Robert", List("CSharp",""), Map("hair"->"red","eye"->"")),
      Row("Washington", null, null),
      Row("Jefferson", List(), Map())
    )

    val schema = new StructType()
      .add("name",StringType)
      .add("languages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    /**
     * root
     * |-- name: string (nullable = true)
     * |-- languages: array (nullable = true)
     * |    |-- element: string (containsNull = true)
     * |-- properties: map (nullable = true)
     * |    |-- key: string
     * |    |-- value: string (valueContainsNull = true)
     */
    df.printSchema()

    /**
     * +----------+--------------+-----------------------------+
     * |name      |languages     |properties                   |
     * +----------+--------------+-----------------------------+
     * |James     |[Java, Scala] |[hair -> black, eye -> brown]|
     * |Michael   |[Spark, Java,]|[hair -> brown, eye ->]      |
     * |Robert    |[CSharp, ]    |[hair -> red, eye -> ]       |
     * |Washington|null          |null                         |
     * |Jefferson |[]            |[]                           |
     * +----------+--------------+-----------------------------+
     */
    // df.show(false)

    /**
     * Exploding List
     *
     * +-------+------+
     * |name   |col   |
     * +-------+------+
     * |James  |Java  |
     * |James  |Scala |
     * |Michael|Spark |
     * |Michael|Java  |
     * |Michael|null  |
     * |Robert |CSharp|
     * |Robert |      |
     * +-------+------+
     *
     * - If we want to keep rows with empty, null List() use explode_outer()
     * - If we want to keep position of the array element, use posexplode()
     * - position of the array element + keep null / empty, use posexplode_outer()
     *
     */
    df.select('name, explode('languages)).show(false)

    /**
     * Exploding Map
     * +-------+----+-----+
     * |name   |key |value|
     * +-------+----+-----+
     * |James  |hair|black|
     * |James  |eye |brown|
     * |Michael|hair|brown|
     * |Michael|eye |null |
     * |Robert |hair|red  |
     * |Robert |eye |     |
     * +-------+----+-----+
     *
     * - If we want to keep rows with empty, null List() use explode_outer()
     * - If we want to keep position of the array element, use posexplode()
     * - position of the array element + keep null / empty, use posexplode_outer()
     */
    df.select('name, explode('properties)).show(false)

  }

}
