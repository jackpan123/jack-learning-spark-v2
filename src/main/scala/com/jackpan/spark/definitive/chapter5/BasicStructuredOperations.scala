package com.jackpan.spark.definitive.chapter5

import org.apache.spark.sql.SparkSession

object BasicStructuredOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicStructuredOperations")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/json/2015-summary.json")

    df.printSchema()
  }
}
