package atech.guide

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._

/**
  * Reference
  * - https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake&language-scala
  */
object DeltaBasic {

  private val spark: SparkSession = SparkSession.builder()
    .appName("DeltaBasic")
    .master("local[*]")
    .getOrCreate()

  private def createData(path: String): Unit = {
    val data = spark.range(0, 5)
    data.write.format("delta").save(path)
  }

  private def readData(path: String): Unit = {
    val df = spark.read.format("delta").load(path)
    df.show()
  }

  private def updateViaOverwrite(path: String): Unit = {
    val data = spark.range(5, 10)
    data.write.format("delta").mode("overwrite").save(path)
  }

  private def conditionalDelete(path: String): Unit = {

    val deltaTable = DeltaTable.forPath(path)
    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))
  }

  private def conditionalUpdate(path: String): Unit = {
    val deltaTable = DeltaTable.forPath(path)

    // Update every even value by adding 100 to it
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100"))
    )
  }

  private def conditionalUpsert(path: String): Unit = {

    val deltaTable = DeltaTable.forPath(path)

    // Upsert (merge) new data
    val newData = spark.range(0, 20).toDF

    deltaTable.as("oldData")
      .merge(newData.as("newData"), "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    deltaTable.toDF.show()
  }

  def main(args: Array[String]): Unit = {

    val path = "./tmp/delta-table"
    readData(path)

  }

}
