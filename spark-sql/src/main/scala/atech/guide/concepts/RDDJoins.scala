package atech.guide.concepts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDJoins {

  private val spark = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val colorsScores: Seq[(String, Int)] = Seq(
    ("blue", 1),
    ("red", 4),
    ("green", 5),
    ("yellow", 2),
    ("orange", 3),
    ("cyan", 0)
  )

  def main(args: Array[String]): Unit = {

    val colorsRDD: RDD[(String, Int)] = spark.sparkContext.parallelize(colorsScores)
    val text = "The sky is blue but the orange pale sun turns from yellow to red"
    val words = text.split(" ").map(_.toLowerCase).map((_, 1))
    val wordsRDD = spark.sparkContext.parallelize(words).reduceByKey(_ + _) // counting word occurrences
    val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD) // implied join type is inner

    scores.foreach(println)

  }

}
