package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 20:31
 */
object ImagesFileOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ImagesFileOperation")
      .getOrCreate()

    val file = "data/cctvVideos/train_images/*"
    val imagesDF = spark.read.format("image").load(file)
    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
      "label").show(5, false)
  }
}
