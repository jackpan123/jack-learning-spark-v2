package com.jackpan.spark.chapter10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, lit}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object BaselineModelExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MachineLearningExample")
      .getOrCreate()

    val filePath = "/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
      "number_of_reviews", "price").show(5)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)
    println(f"""There are ${trainDF.count} rows in the training set, and ${testDF.count} in the test set""")

    val avgPrice = trainDF.select(avg("price")).first().getDouble(0)
    val predDF = testDF.withColumn("avgPrediction", lit(avgPrice))

    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("avgPrediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator.evaluate(predDF)
    println(f"The RMSE for predicting the average price is $rmse%.2f")
  }
}
