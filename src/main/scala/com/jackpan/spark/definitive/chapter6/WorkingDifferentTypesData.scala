package com.jackpan.spark.definitive.chapter6

import org.apache.spark.sql.SparkSession

object WorkingDifferentTypesData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WorkingDifferentTypesData")
      .getOrCreate()


    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/retail-data/by-day/2010-12-01.csv")

    df.printSchema()
    df.createOrReplaceTempView("dfTable")

  }
}
