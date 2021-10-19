package com.jackpan.spark.chapter6

import scala.util.Random._
import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/19 12:37
 */
object SampleDatasetsExample {

  case class Usage(uid:Int, uname:String, useage:Int)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SampleDatasetsExample")
      .getOrCreate()

    import spark.implicits._
    val r = new scala.util.Random(42)

    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

    val dsUsage = spark.createDataset(data)
    dsUsage.show(10)
  }
}
