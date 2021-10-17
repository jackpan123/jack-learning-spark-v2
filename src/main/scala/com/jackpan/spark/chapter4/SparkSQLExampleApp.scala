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

    df.select("distance", "origin", "destination")
        .where(col("distance") > 1000)
        .orderBy(desc("distance")).show(10)

    spark.sql(
      """SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' ORDER BY delay DESC""")
      .show(10)

    df.select("date","delay", "origin", "destination")
        .where(col("delay") > 120
          and col("ORIGIN").equalTo( "SFO" )
          and col("DESTINATION").equalTo( "ORD" ))
        .orderBy(desc("delay"))
        .show(10)


    df.withColumn("newDate", to_timestamp(col("date"), "MMddHHmm"))
      .drop("date").createOrReplaceTempView("us_delay_flights_table")
    spark.sql(
      """SELECT MONTH(newDate) FROM us_delay_flights_table WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' GROUP BY MONTH(newDate) ORDER BY MONTH(newDate) DESC""")
      .show(10)

    spark.sql(
      """SELECT delay, origin, destination,
              CASE
                  WHEN delay > 360 THEN 'Very Long Delays'
                  WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
                  WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
                  WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                  WHEN delay = 0 THEN 'No Delays'
                  ELSE 'Early'
               END AS Flight_Delays
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC
         """).show(10)

    df.select("delay", "origin", "destination")
      .withColumn("Flight_Delays", when(col("delay") > 360, "Very Long Delays")
      .when(col("delay") >= 120 and col("delay") < 360, "Long Delays")
        .when(col("delay") >= 60 and col("delay") < 120, "Short Delays")
          .when(col("delay") > 0 and col("delay") < 60, "Tolerable Delays")
            .when(col("delay").equalTo(0), "No Delays")
            .otherwise("Early"))
      .orderBy(col("origin").asc, col("delay").desc)
      .show(10)

  }
}
