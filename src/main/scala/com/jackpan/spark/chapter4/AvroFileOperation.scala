package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 20:15
 */
object AvroFileOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("CSVFileOperation")
      .getOrCreate()

    val file = """data/flights/summary-data/avro/*"""
    val df = spark.read.format("avro")
      .load(file)

    df.show(false)

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW episode_tbl
    USING avro
    OPTIONS (path "data/flights/summary-data/avro/*")""")

    spark.sql("SELECT * FROM episode_tbl").show(false)

    df.write.format("avro")
      .mode("overwrite")
      .save("/Users/jackpan/JackPanDocuments/temporary/df_avro")
  }
}
