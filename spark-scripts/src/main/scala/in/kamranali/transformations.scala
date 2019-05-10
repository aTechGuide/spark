package in.kamranali

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object transformations {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create Spark context using every core of local machine
    // Remember: SparkContext object is used to create our RDDs
    val sc = new SparkContext("local[*]", "Transformation")


//    val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
//    val group = data.groupByKey().collect()
//    group.foreach(println)


//    val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
//    val reducedData = sc.parallelize(words).map(w => (w,1)).reduceByKey((a,b) => a+b)
//    reducedData.foreach(println)

    val data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8), ('d',8)))
    val result = data.join(data2)
    // result.foreach(println)
    println(result.collect().mkString(","))
  }
}
