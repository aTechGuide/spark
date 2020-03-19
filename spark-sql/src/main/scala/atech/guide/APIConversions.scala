package atech.guide

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object APIConversions {

  def rddToDF(stringRDD: RDD[String], spark: SparkSession) = {

    import spark.implicits._

    stringRDD.toDF("Strings")
  }

  def rddToDS(stringRDD: RDD[String], spark: SparkSession) = {

    // We need implicits for encoders
    import spark.implicits._

    spark.createDataset(stringRDD)
  }

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("APIConversions")
      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext

    val stringRDD: RDD[String] = sc.parallelize(Seq("Kamran", "Ali", "nit"))


    // RDD -> DF
    // We lose Type Info but get SQL Capability
    val stringDF = rddToDF(stringRDD, spark)


    // RDD -> DS
    // We get Type Info + SQL Capability
    val stringDS = rddToDS(stringRDD, spark)

  }
}
