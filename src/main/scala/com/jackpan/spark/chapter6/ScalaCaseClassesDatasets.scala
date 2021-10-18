package com.jackpan.spark.chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders


/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/18 23:05
 */
object ScalaCaseClassesDatasets {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CommonDataFrameOperations")
      .getOrCreate()
    implicit val blogger =  Encoders.product[Bloggers]
    case class Bloggers(Id:Int, First:String, Last:String, Url:String, Date:String,
                        Hits: Int, Campaigns:Array[String])

    val bloggers = "data/blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]
  }
}
