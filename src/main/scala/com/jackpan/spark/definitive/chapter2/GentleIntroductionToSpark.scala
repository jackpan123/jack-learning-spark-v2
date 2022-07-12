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
  }


}
