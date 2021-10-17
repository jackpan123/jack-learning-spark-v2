package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 12:20
 */
object TableOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("TableOperation")
      .getOrCreate()

//    spark.sql("CREATE DATABASE learn_spark_db")
//    spark.sql("USE learn_spark_db")

    val csvFile = "data/departuredelays.csv"
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val flights_df = spark.read.schema(schema).csv(csvFile)
    // Create managed table
    //flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

    // Create unmanaged table
    flights_df
      .write
      .option("path", "/Users/jackpan/JackPanDocuments/temporary/us_flights_delay")
      .saveAsTable("us_delay_flights_tbl")

  }
}
