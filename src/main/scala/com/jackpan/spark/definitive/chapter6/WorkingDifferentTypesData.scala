package com.jackpan.spark.definitive.chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    df.select(lit(5), lit("five"), lit(5.0))

    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where(col("InvoiceNo") === (536365))
      .select("InvoiceNo", "Description")
      .show(5, false)

    df.where("InvoiceNo = 536365")
      .show(5, false)

    df.where("InvoiceNo <> 536365")
      .show(5, false)


    val priceFilter = col("UnitPrice") > 600
    val descripFilter = col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter))
      .show()

    val DOTCodeFilter = col("StockCode") === "DOT"

    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
      .where("isExpensive")
      .select("unitPrice", "isExpensive").show(5)

    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "UnitPrice").show(5)

    // Use equals is null safe!
    df.where(col("Description").eqNullSafe("hello")).show()


  }
}
