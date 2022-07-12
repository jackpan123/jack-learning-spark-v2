package com.jackpan.spark.definitive.chapter2

import org.apache.spark.sql.SparkSession

object GentleIntroductionToSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GentleIntroductionToSpark")
      .getOrCreate()

    val myRange = spark.range(1000).toDF("number")
    myRange.show(10)

    val divideBy2 = myRange.where("number % 2 == 0")
    divideBy2.show(10)
    println(divideBy2.count())

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Volumes/JackApp/jackproject/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv")

    // You can take some row from DataFrame
    flightData2015.take(3)

    // By default, spark perform a shuffle, Spark out put 200 partitions and
    // you can set conf to reduce output partitions.
//    spark.conf.set("spark.sql.shuffle.partitions", "5")

    flightData2015.sort("count").explain()
  }


}