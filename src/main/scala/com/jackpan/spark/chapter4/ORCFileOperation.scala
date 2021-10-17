package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 20:23
 */
object ORCFileOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ORCFileOperation")
      .getOrCreate()

    val file = "data/flights/summary-data/orc/*"
    val df = spark.read.format("orc").load(file)

    df.show(10, false)

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_orc
                    USING orc
                    OPTIONS (
                      path "data/flights/summary-data/orc/*"
                    )""")

    spark.sql("SELECT * FROM us_delay_flights_tbl_orc").show()

    df.write.format("json")
      .mode("overwrite")
      .save("/Users/jackpan/JackPanDocuments/temporary/df_orc")
  }
}
