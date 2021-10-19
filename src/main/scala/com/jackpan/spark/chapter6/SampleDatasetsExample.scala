package com.jackpan.spark.chapter6

import scala.util.Random._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/19 12:37
 */
object SampleDatasetsExample {

  case class Usage(uid:Int, uname:String, usage:Int)
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

    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    def filterWithUsage(u: Usage) = u.usage > 900

    dsUsage
      .filter(filterWithUsage(_))
      .orderBy(desc("usage"))
      .show(5, false)

    dsUsage.map(u => {
      if (u.usage > 750) {
        u.usage * .15
      } else {
        u.usage * .50
      }
    }).show(5, false)

    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) {
        usage * .15
      } else {
        usage * .50
      }
    }

    dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)
  }
}
