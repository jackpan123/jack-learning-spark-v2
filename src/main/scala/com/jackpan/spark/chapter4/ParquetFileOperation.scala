package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 18:48
 */
object ParquetFileOperation {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("ParquetFileOperation")
      .getOrCreate()
    val file = """data/flights/summary-data/parquet/2010-summary.parquet/"""
    val df = spark.read.format("parquet").load(file)

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
                    USING parquet
                    OPTIONS (
                      path "data/flights/summary-data/parquet/2010-summary.parquet/" )""")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()

    df.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save("/Users/jackpan/JackPanDocuments/temporary/df_parquet")

    df.write.format("parquet")
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl")

    spark.sql("""SELECT * FROM us_delay_flights_tbl""").show()
  }
}
