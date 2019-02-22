package in.kamranali.fakeFriends

import in.kamranali.fakeFriends.SparkSQL.mapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrames {

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String): Person = {

    val fields = line.split(',')

    val person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)

    return person
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("SparkSQL").master("local[*]").getOrCreate()

    // Loading Unstructured data
    val lines = spark.sparkContext.textFile("./src/main/resources/fakefriends/fakefriends.csv")

    // Before converting a Structured RDD into a dataset do following import
    import spark.implicits._

    val people = lines.map(mapper).toDS().cache()

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
