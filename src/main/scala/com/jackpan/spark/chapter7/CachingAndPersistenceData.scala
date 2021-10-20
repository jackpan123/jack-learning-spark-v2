package com.jackpan.spark.chapter7

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/20 13:20
 */
object CachingAndPersistenceData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CachingAndPersistenceData")
      .getOrCreate()

    val df = spark.range(1 * 10000000).toDF("id")
      .withColumn("square", col("id") * col("id"))

    df.cache()
    df.count()

    df.count()
  }
}
