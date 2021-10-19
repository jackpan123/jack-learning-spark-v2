package com.jackpan.spark.chapter6

import org.apache.spark.sql.SparkSession


/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/18 23:05
 */
object ScalaCaseClassesDatasets {
  case class Bloggers(id:Long, first:String, last:String, url:String, Published:String,
                      hits: Long, campaigns:Array[String])

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ScalaCaseClassesDatasets")
      .getOrCreate()
    import spark.implicits._
    val bloggers = "data/blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load()
      .as[Bloggers]

    bloggersDS.show()
  }
}
