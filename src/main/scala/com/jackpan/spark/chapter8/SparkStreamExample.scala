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

    val lines = spark
      .readStream.format("socket") .option("host", "localhost") .option("port", 9999) .load()
    val words = lines.select(split(col("value"), "\\s").as("word"))
    val counts = words.groupBy("word").count()
    val checkpointDir = "/Users/jackpan/JackPanDocuments/temporary/checkpoints"
    val streamingQuery = counts.writeStream
      .format("console")
      .outputMode("complete") .trigger(Trigger.ProcessingTime("1 second")) .option("checkpointLocation", checkpointDir) .start()
    streamingQuery.awaitTermination()

  }
}
