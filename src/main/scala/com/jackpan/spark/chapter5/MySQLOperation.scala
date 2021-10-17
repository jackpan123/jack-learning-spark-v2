package com.jackpan.spark.chapter5

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 21:29
 */
object MySQLOperation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MySQLOperation")
      .getOrCreate()

    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/api-deploy")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "sys_menu")
      .option("user", "root")
      .option("password", "1234")
      .load()

    jdbcDF.show()

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/api-deploy")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "sys_menuss")
      .option("user", "root")
      .option("password", "1234")
      .save()
  }
}
