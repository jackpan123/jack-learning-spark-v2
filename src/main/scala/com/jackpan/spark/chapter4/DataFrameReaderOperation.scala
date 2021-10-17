package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 18:13
 */
object DataFrameReaderOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TableOperation")
      .getOrCreate()
    val file = "data/flights/summary-data/parquet/2010-summary.parquet"

    val df = spark.read.format("parquet").load(file)

    val df2 = spark.read.load(file)

    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load("data/flights/summary-data/csv/*")

    val df4 = spark.read.format("json")
      .load("data/flights/summary-data/json/*")
  }
}
