package com.jackpan.spark.chapter10

import org.apache.spark.sql.SparkSession

object MachineLearningExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MachineLearningExample")
      .getOrCreate()

    val filePath = "/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
      "number_of_reviews", "price").show(5)

    val Array(trainDF, testDf) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)
    println(f"""There are ${trainDF.count} rows in the training set, and ${testDf.count} in the test set""")
  }
}
