package in.kamranali.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/*
In this script we are using StructType to apply schema to raw data
 */

object DataFrameStructs {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("SparkSchemaWithStructs").master("local[*]").getOrCreate()

    // Loading Unstructured data
    val friends = spark.sparkContext.textFile("./src/main/resources/fakefriends/fakefriends.csv")

    val schema = StructType(
      Seq(
        StructField("ID", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("numFriends", IntegerType, true)
      )
    )

    // Creating a Row RDD from raw friends data i.e. applying Schema
    val rowRDD = friends.map(_.split(",")).map(e ⇒ Row(e(0).toInt, e(1), e(2).toInt, e(3).toInt))

    // Before converting a Structured RDD into a dataset do following import
    // import spark.implicits._

    val people = spark.createDataFrame(rowRDD, schema)

    println("Printing Inferred Schema")
    people.printSchema()

    people.select("name").show()

    people.filter(people("age") < 21).show()

    people.groupBy("age").count().show()

    // Making everyone 10 years older
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }

}
