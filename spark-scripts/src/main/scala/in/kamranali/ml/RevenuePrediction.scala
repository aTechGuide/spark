package in.kamranali.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

object RevenuePrediction {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RevenuePredictionLinearRegrssionApp")

    /*
    This reads data in x,y. where x = "label" we want to predict and y = "feature" which we can use to predict the label.
    For MLLib to work properly, we need to scale the data to fit it -1 to 1 range with 0 mean
     */
    val trainingLines = sc.textFile("./src/main/resources/ml/regression.txt")

    /*
    Another RDD containing "test" data that we want to predict values for using linear model. It expects both label and feature data.
    In real world, we don't know correct value and would input just feature data.
     */
    val testingLines = sc.textFile("./src/main/resources/ml/regression.txt")

    // Converting input data to LabeledPoints for MLLib
    val trainingData = trainingLines.map(LabeledPoint.parse).cache()
    val testData = testingLines.map(LabeledPoint.parse)

    // Creating Linear Regression model
    // We may need to keep on trying with all the permutations of magic numbers until we get good results.
    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(1.0)
      .setUpdater(new SquaredL2Updater)
      .setRegParam(0.01)

    val model = algorithm.run(trainingData)

    // Predict values for out test features data using linear regression model
    val predictions = model.predict(testData.map(_.features))

    // We are "Zipping" in real values so we can compate them
    val predictionAndLabel = predictions.zip(testData.map(_.label))

    // Printing predicted and actual values for each data point
    for (prediction <- predictionAndLabel) {
      println(prediction)
    }
  }

}
