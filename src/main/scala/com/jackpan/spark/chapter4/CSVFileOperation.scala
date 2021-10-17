package com.jackpan.spark.chapter4

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/17 19:11
 */
object CSVFileOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("CSVFileOperation")
      .getOrCreate()
    val file = """data/flights/summary-data/csv/*"""
    val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

    val df = spark.read.format("csv")
      .schema(schema)
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("nullValue", "")
      .load(file)

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl_csv
                    USING csv
                    OPTIONS (
                      path "data/flights/summary-data/csv/*",
                      header "true",
                      inferSchema "true",
                      mode "FAILFAST"
                    )""")

    spark.sql("SELECT * FROM us_delay_flights_tbl_csv").show(10)

    df.write.format("csv").mode("overwrite").save("/Users/jackpan/JackPanDocuments/temporary/df_csv")
  }
}
