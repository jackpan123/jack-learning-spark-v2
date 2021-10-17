package com.jackpan.spark.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 22:16
 */
object HigherOrderFunctions {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("HigherOrderFunctions")
      .getOrCreate()
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    import spark.implicits._
    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")
    tC.show()

    spark.sql(
      """SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
        FROM tC """).show()

    spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

    spark.sql("""SELECT celsius, exists(celsius, t -> t = 38) as threshold FROM tC""").show()

    spark.sql(
      """SELECT celsius, reduce(
        celsius,
        0,
        (t, acc) -> t + acc,
        acc -> (acc div size(celsius) * 9 div 5) + 32
      ) as avgFahrenheit
      FROM tC""").show()
  }
}
