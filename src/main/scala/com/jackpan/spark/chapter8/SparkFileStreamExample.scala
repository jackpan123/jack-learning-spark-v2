package com.jackpan.spark.chapter8
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/23 18:00
 */
object SparkFileStreamExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkFileStreamExample")
      .getOrCreate()

    val inputDirectoryOfJsonFiles = "data/flights/summary-data/json"
    val fileSchema = new StructType()
      .add("ORIGIN_COUNTRY_NAME", StringType)
      .add("DEST_COUNTRY_NAME", StringType)
      .add("count", IntegerType)

    val inputDF = spark.readStream
      .format("json")
      .schema(fileSchema)
      .load(inputDirectoryOfJsonFiles)

    val checkpointDir = "/Users/jackpan/JackPanDocuments/temporary/checkpoints"
    val outputDir = "/Users/jackpan/JackPanDocuments/temporary/outputpath"
    val streamingQuery = inputDF
      .writeStream
      .format("json")
      .option("path", outputDir)
      .option("checkpointLocation", checkpointDir)
      .start()

    streamingQuery.awaitTermination()

  }
}
