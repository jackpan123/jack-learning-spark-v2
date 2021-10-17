package com.jackpan.spark.chapter5

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 20:55
 */
object UserDefinedFunctionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("UserDefinedFunctionExample")
      .getOrCreate()

    val cubed = (s: Long) => {
      s * s * s
    }

    spark.udf.register("cubed", cubed)

    spark.range(1, 9).createOrReplaceTempView("udf_test")
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
  }
}
