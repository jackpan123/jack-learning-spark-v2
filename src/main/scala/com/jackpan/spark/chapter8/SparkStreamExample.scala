package com.jackpan.spark.chapter8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/21 22:16
 */
object SparkStreamExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStreamExample")
      .getOrCreate()

    val delaysPath =
      "data/departuredelays.csv"
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val lines = spark.readStream
      .schema(schema)
      .format("csv")
      .option("header", "true")
      .load(delaysPath)

    val words = lines.select(split(col("date"), "0").as("word"))
    val counts = words.groupBy("word").count()

    val checkpointDir = "/Users/jackpan/JackPanDocuments/temporary"
    val streamingQuery = counts.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", checkpointDir)
      .start()

    streamingQuery.awaitTermination()
  }
}
