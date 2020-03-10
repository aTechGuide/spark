package atech.guide

import org.apache.spark.sql.SparkSession


object SparkSQL {

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String): Person = {

    val fields = line.split(',')

    val person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)

    return person
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Loading Unstructured data
    val lines = spark.sparkContext.textFile("src/main/resources/fakefriends/fakefriends.csv")

    // Converting it into Strutured Data
    val people = lines.map(mapper)

    // Before converting a Structured RDD into a dataset do following import
    import spark.implicits._

    // Getting a dataset
    val schemaPeople = people.toDS()

    schemaPeople.printSchema()

    /*
    Main SQL Thing begins here
     */

    // Converting contents of DataSet into a SQL table with name People
    schemaPeople.createOrReplaceTempView("people")

    // After registering a DataFrame as a table we can run SQL queries
    val teenagers = spark.sql("SELECT * FROM people WHERE age >=13 AND age <=19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }

}
