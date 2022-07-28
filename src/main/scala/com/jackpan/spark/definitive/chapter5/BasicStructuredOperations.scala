package com.jackpan.spark.definitive.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions._


object BasicStructuredOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicStructuredOperations")
      .getOrCreate()

//    val df = spark.read.format("json")
//      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/json/2015-summary.json")
//
//    df.printSchema()

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("json").schema(myManualSchema).
      load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/json/2015-summary.json")
    df.printSchema()


    df.show()

    df.select(col("DEST_COUNTRY_NAME"), column("count")).show()
    df.select(col("DEST_COUNTRY_NAME"), df.col("count") + 2).show()

    df.select(expr("count + 1")).show()

    // The difference between use col function and use expr function
    df.select(((((col("count") + 5) * 200) -6)) < col("count")).show()
    df.select(expr("(((count + 5) * 200) -6) < count")).show()
//    $"myColumn"
//    'myColumn

    // Get DataFrame columns
    val columns = df.columns
    for (ele <- columns) {
      println(ele)
    }

  }
}
