package com.jackpan.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object SomeExamples {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SomeExamples")
      .getOrCreate()

    val dataDF = spark.createDataFrame(Seq(("2022", "12", "09"), ("2022", "12", "19"),
      ("2022", "12", "15"))).toDF("year", "month", "day")

    dataDF.withColumn("dateStr",
      concat(col("year"), lit("-"),col("month"), lit("-"), col("day")))
      .withColumn("date", to_date(col("dateStr"), "yyyy-MM-dd"))
      .show(false)


  }
}
