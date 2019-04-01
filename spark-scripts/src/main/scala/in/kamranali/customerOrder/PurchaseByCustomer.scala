package in.kamranali.customerOrder

import org.apache.spark.SparkContext

// Find Amount spent by Each customer
object PurchaseByCustomer {

  def main(args: Array[String]): Unit = {

    //Logger

    val sc = new SparkContext("local[*]", "PurchaseByCustomer")

    // Read the data file
    // Loading data into RDD constant lines
    // CustomerID, ItemID, AmountSpentOnThatItem
    val lines = sc.textFile("./src/main/resources/customerdata/customer-orders.csv")

    val purchase = lines.map(lines => {
      val fields = lines.split(",")
      (fields(0).toInt, fields(2).toFloat)
    })

     val res = purchase.reduceByKey((x,y) => x+y)

    // Invert Tuple to sort based on amount spend
    val invertedSort = res.map(x => (x._2, x._1)).sortByKey()

    invertedSort.collect().foreach(println)

  }

}
