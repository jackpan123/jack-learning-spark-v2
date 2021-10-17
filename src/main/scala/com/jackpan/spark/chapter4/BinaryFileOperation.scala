package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 20:37
 */
object BinaryFileOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BinaryFileOperation")
      .getOrCreate()

    val file = "data/cctvVideos/train_images/*"
    val binaryFilesDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .load(file)

    binaryFilesDF.show(5)
  }
}
