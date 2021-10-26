package com.jackpan.spark.chapter8

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/26 12:53
 */
object StreamJoinExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StreamJoinExample")
      .getOrCreate()

    // Reading from your static data source
    val impressionsStatic = spark.read.load()

    // Reading from your stream source
    val clicksStream = spark.readStream.load()

    val matched = clicksStream.join(impressionsStatic, "adId")

    val matched1 = clicksStream.join(impressionsStatic, Seq("adId"), "leftOuter")

    val impressions = spark.readStream.load()
    // Streaming DataFrame[adId: String, clickTime: Timestamp, ...]
    val clicks = spark.readStream.load()
    val matched2 = impressions.join(clicks, "adId")

    val impressionsWithWatermark = impressions
      .selectExpr("adId As impressionAdId", "impressionTime")
      .withWatermark("impressionTime", "2 hours")

    val clicksWithWatermark = clicks
      .selectExpr("adId AS clickAdId", "clickTime")
      .withWatermark("clickTime", "3 hours")

    impressionsWithWatermark.join(clicksWithWatermark,
      expr(
        """
          clickAdId = impressionAdId AND
          clickTime BETWEEN impressionTime AND  impressionTime + interval 1 hour"""))
  }
}
