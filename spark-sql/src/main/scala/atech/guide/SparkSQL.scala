package atech.guide

import org.apache.spark.sql.{Dataset, SparkSession}


object SparkSQL {

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String): Person = {

    val fields = line.split(',')

    Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Loading Unstructured data
    val lines = spark.sparkContext.textFile("src/main/resources/data/fakefriends/fakefriends.csv")

    // Converting it into Structured Data
    val people = lines.map(mapper)

    // Before converting a Structured RDD into a dataset do following import
    import spark.implicits._

    // Getting a dataset
    val schemaPeople: Dataset[Person] = people.toDS()

    // 1
    // After registering a DataFrame as a table we can run SQL queries
    // Converting contents of DataSet into a SQL table with name People
    schemaPeople.createOrReplaceTempView("people")
    val teenagers = spark.sql("SELECT * FROM people WHERE age >=13 AND age <=19")
    println(teenagers.count())

    // 2 Doing without SQL
    val count = schemaPeople.filter($"age" >= 13 && $"age" <= 19).count()
    println(s"Count = $count")

    // 3
    schemaPeople.filter(_.age >= 13).filter(_.age <= 19).count()
  }

}
