package com.jackpan.spark.chapter3

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/15 13:09
 */
object RowExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("RowExample")
      .getOrCreate()
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))

    blogRow(1)


    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = spark.createDataFrame(rows).toDF("Authors", "State")
    authorsDF.show()
  }
}
