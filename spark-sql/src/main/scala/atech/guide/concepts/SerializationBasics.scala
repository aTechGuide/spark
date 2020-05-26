package atech.guide.concepts

/**
 * Ref -> https://medium.com/onzo-tech/serialization-challenges-with-spark-and-scala-a2287cd51c54
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class Example1 {
  val num = 1

  def myFunc(testRdd: RDD[Int]): Unit = {
    val func1: Int => Int = value => value + 1
    val res = testRdd.map(func1) // Only func1 Object is getting serialized not `SerializationBasics`
    res.take(5).foreach(println)
  }
}

class Example2Error {
  val num = 1

  def myFunc(testRdd: RDD[Int]): Unit = {
    val res = testRdd.map(_ + num).collect.toList
    res.take(5).foreach(println)
  }
}

class Example2Fixed extends Serializable {
  val num = 1

  def myFunc(testRdd: RDD[Int]): Unit = {
    val res = testRdd.map(_ + num).collect.toList
    res.take(5).foreach(println)
  }
}

object SerializationBasics {

  private val spark = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  private val basicRDD: RDD[Int] = spark.sparkContext.parallelize(Seq[Int](1,2,3,4,5))

  def main(args: Array[String]): Unit = {

    // new Example1().myFunc(basicRDD)
    // new Example2Error().myFunc(basicRDD) // Fails with Task NOT Serializable
    new Example2Fixed().myFunc(basicRDD)

  }
}