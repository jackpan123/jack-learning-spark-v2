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

    val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

    df.selectExpr("CustomerId",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice"))
      .show(5)

    df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
    println(df.stat.corr("Quantity", "UnitPrice"))
    df.select(corr("Quantity", "UnitPrice")).show()


    df.describe().show()

    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    println(df.stat.approxQuantile("UnitPrice", quantileProbs, relError).head)

    df.stat.crosstab("StockCode", "Quantity").show()

    df.stat.freqItems(Seq("StockCode", "Quantity")).show()

    df.select(monotonically_increasing_id()).show(2)
  }
}
