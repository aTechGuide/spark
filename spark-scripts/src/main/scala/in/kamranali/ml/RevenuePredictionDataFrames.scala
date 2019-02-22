package in.kamranali.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object RevenuePrediction {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("RevenuePredictionDF").master("local[*]").getOrCreate()

    /*
    This reads data in x,y. where x = "label" we want to predict (First column of our data) and y = "feature" (second column) which we can use to predict the label.
    We can have more than one feature which is why vector is required.
     */
    val inputLines = spark.sparkContext.textFile("./src/main/resources/ml/regression.txt")
    val data = inputLines.map(_.split(",")).map( x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))

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

    val predictionAndLabel = fullPrecidtions.select("preduction", "label").rdd.map(x => (x.getDouble(0), x.getDouble(1)))

    // Printing predicted and actual values for each data point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    spark.stop()
  }

}
