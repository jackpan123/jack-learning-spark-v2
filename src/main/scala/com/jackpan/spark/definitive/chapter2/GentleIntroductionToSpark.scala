package com.jackpan.spark.definitive.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GentleIntroductionToSpark {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GentleIntroductionToSpark")
      .getOrCreate()

    val myRange = spark.range(1000).toDF("number")
    myRange.show(10)

    val divideBy2 = myRange.where("number % 2 == 0")
    divideBy2.show(10)
    println(divideBy2.count())

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Volumes/JackApp/jackproject/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv")

    // You can take some row from DataFrame
    flightData2015.take(3)

    // By default, spark perform a shuffle, Spark out put 200 partitions and
    // you can set conf to reduce output partitions.
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    flightData2015.sort("count").explain()

    flightData2015.createOrReplaceTempView("flight_data_2015")

    val sqlWay = spark.sql(
      """
        SELECT DEST_COUNTRY_NAME, count(1)
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        """)

    val dataFrameWay = flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .count()

    sqlWay.explain
    dataFrameWay.explain

    // Find max count in the dataset. It is 370002
    val rows = spark.sql("""SELECT max(count) from flight_data_2015""").take(1)
    // Array foreach
    rows.foreach(row => println(row.getInt(0)))
    val rows1 = flightData2015.select(max("count")).take(1)
    rows1.foreach(row => println(row.toString()))

    val maxSql = spark.sql(
      """
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
        """)

    maxSql.show()

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total")).limit(5).show()


    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total")).limit(5).explain()

  }


}
