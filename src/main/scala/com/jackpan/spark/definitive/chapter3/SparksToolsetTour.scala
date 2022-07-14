package com.jackpan.spark.definitive.chapter3

import org.apache.spark.sql.SparkSession



case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

object SparksToolsetTour {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparksToolsetTour")
      .getOrCreate()

    import spark.implicits._
    val flightsDF = spark.read.parquet("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/parquet/2010-summary.parquet")

    val flights = flightsDF.as[Flight]


    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)

    flights.take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/retail-data/by-day/*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema
  }
}
