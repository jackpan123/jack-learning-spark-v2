package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 18:48
 */
object JSONFileOperation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("JSONFileOperation")
      .getOrCreate()
    val file = """data/flights/summary-data/json/*"""
    val df = spark.read.format("json").load(file)

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
                    USING json
                    OPTIONS (
                      path "data/flights/summary-data/json/*" )""")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    df.write.format("json")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/Users/jackpan/JackPanDocuments/temporary/df_json")

//    df.write.format("json")
//      .mode("overwrite")
//      .saveAsTable("us_delay_flights_tbl")
//
//    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()
  }
}
