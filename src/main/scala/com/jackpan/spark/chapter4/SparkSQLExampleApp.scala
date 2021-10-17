package com.jackpan.spark.chapter4
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
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

    spark.sql(
      """SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER BY delay DESC""")
      .show(10)

    df.withColumn("newDate", to_timestamp(col("date"), "MMddHHmm"))
      .drop("date").createOrReplaceTempView("us_delay_flights_table")
    spark.sql(
      """SELECT MONTH(newDate) FROM us_delay_flights_table WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' GROUP BY MONTH(newDate) ORDER BY MONTH(newDate) DESC""")
      .show(10)


  }
}
