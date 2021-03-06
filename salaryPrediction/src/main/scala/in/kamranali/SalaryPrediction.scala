package in.kamranali

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object SalaryPrediction {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session Interface in spark 2.0
    val spark = SparkSession.builder
      .appName("SalaryPredictionDF")
      .master("local[*]") // using all cores of CPU
      .getOrCreate()

    val inputLines = spark.sparkContext.textFile("./src/main/resources/salary_data.txt")
    val data = inputLines.map(_.split(",")).map( x => (x(1).toDouble, Vectors.dense(x(0).toDouble)))

    // Converting data to DataFrame
    import spark.implicits._

    // Creating Sequence of Column Names
    val colNames = Seq("label", "features")
    val df = data.toDF(colNames: _*) // Converting Sequence to Variable Argument list using _* operator

    // Splitting our data into training and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Creating linear regression model
    val lir = new LinearRegression()
      .setRegParam(0.3) // regularization
      .setElasticNetParam(0.8)  // elastic net mixing
      .setMaxIter(100) // max iteration
      .setTol(1E-6) // convergence tolerance

    // Train the model using training data
    val model = lir.fit(trainingDF)

    // Predicting values on our test data
    // This will add a prediction column to test data
    val fullPrecidtions = model.transform(testDF).cache()

    val predictionAndLabel = fullPrecidtions.select("prediction", "label").rdd.map(x => (x.getDouble(0).round, x.getDouble(1).round))

    // Printing predicted and actual values for each data point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    spark.stop()
  }

}
