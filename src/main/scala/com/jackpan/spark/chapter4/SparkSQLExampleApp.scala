package com.jackpan.spark.chapter4
import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/16 23:14
 */
object SparkSQLExampleApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SparkSQLExampleApp")
      .getOrCreate()

    val csvFile = "data/departuredelays.csv"

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val df = spark.read
      .schema(schema)
      .format("csv")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")
    spark.sql("""SELECT distance, origin, destination FROM us_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""")
      .show(10)
  }
}
