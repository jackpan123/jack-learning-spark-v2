package com.jackpan.spark.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 22:51
 */
object CommonRelationalOperations {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("CommonRelationalOperations")
      .getOrCreate()

    val delaysPath =
      "data/departuredelays.csv"
    val airportsPath =
      "data/flights/airport-codes-na.txt"

    val airports = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)

    airports.createOrReplaceTempView("airports_na")

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val delays = spark.read
      .schema(schema)
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    val foo = delays.filter(
      expr(
        """origin == 'SEA' AND destination == 'SFO' AND
          date like '01010%' AND delay > 0"""))

    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo LIMIT 10").show()

    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(
      expr(
        """origin == 'SEA' AND destination == 'SFO' AND
          date like '01010%' AND delay > 0""")).show()

    foo.join(
      airports.as("air"),
      col("air.IATA") === col("origin")
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    val delayWindowsDF = delays.select("origin", "destination", "delay")
        .where(col("origin") isin("SEA", "SFO", "JFK")
          and(col("destination") isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL")))
        .groupBy(col("origin"), col("destination"))
        .agg(sum("delay") as "TotalDelays")
    delayWindowsDF.createOrReplaceTempView("departureDelaysWindow")
    spark.sql("SELECT * FROM departureDelaysWindow").show()

    spark.sql(
      """
        SELECT origin, destination, ToTalDelays, rank
          FROM (
          SELECT origin, destination, ToTalDelays, dense_rank()
            OVER (PARTITION BY origin ORDER BY ToTalDelays DESC) as rank
            FROM departureDelaysWindow
          ) t
          WHERE rank <= 3
        """).show()

    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )

    foo2.show()

    val foo3 = foo2.drop("delay")
    foo3.show()

    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()
  }
}
