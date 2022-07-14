package com.jackpan.spark.definitive.chapter3

import org.apache.spark.sql.SparkSession

object SparksToolsetTour {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparksToolsetTour")
      .getOrCreate()

    import spark.implicits._

    case class Flight(DEST_COUNTRY_NAME: String,
                      ORIGINAL_COUNTRY_NAME: String,
                      count: BigInt)

    val flightsDF = spark.read.parquet("/Volumes/JackApp/jackproject/Spark-The-Definitive-Guide/data/flight-data/parquet/2010-summary.parquet/")
    flightsDF.show()
  }
}
