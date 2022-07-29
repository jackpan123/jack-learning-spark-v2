package com.jackpan.spark.definitive.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object BasicStructuredOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BasicStructuredOperations")
      .getOrCreate()

//    val df = spark.read.format("json")
//      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/json/2015-summary.json")
//
//    df.printSchema()

    val myManualSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      StructField("count", LongType, false, Metadata.fromJson("{\"hello\":\"world\"}"))
    ))

    val df = spark.read.format("json").schema(myManualSchema).
      load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/json/2015-summary.json")
    df.printSchema()


    df.show()

    df.select(col("DEST_COUNTRY_NAME"), column("count")).show()
    df.select(col("DEST_COUNTRY_NAME"), df.col("count") + 2).show()

    df.select(expr("count + 1")).show()

    // The difference between use col function and use expr function
    df.select(((((col("count") + 5) * 200) -6)) < col("count")).show()
    df.select(expr("(((count + 5) * 200) -6) < count")).show()
//    $"myColumn"
//    'myColumn

    // Get DataFrame columns
    val columns = df.columns
    for (ele <- columns) {
      println(ele)
    }

    // Get first row record
    val row = df.first()
    println(row.getLong(2))

    // Create you own row
    val myRow = Row("Hello", null, 1L)
    myRow(0)
    myRow(0).asInstanceOf[String]
    println(myRow.getString(0))
    println(myRow.getLong(2))

    df.createOrReplaceTempView("dfTable")
    spark.sql("Select * from dfTable where count > 20").show()


    val myOwnSchema = new StructType(Array(
      new StructField("some", StringType, true),
      new StructField("col", StringType, true),
      new StructField("names", LongType, false)
    ))

    val myRows = Seq(myRow)
    val myRDD = spark.sparkContext.parallelize(myRows)
    val myDf = spark.createDataFrame(myRDD, myOwnSchema)
    myDf.show()

//    val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

    df.select("DEST_COUNTRY_NAME").show(2)

    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

    df.select(
      df.col("DEST_COUNTRY_NAME"),
      col("DEST_COUNTRY_NAME"),
      column("DEST_COUNTRY_NAME"),
      expr("DEST_COUNTRY_NAME")
    ).show(2)

    df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

    df.select(expr("DEST_COUNTRY_NAME AS destination").alias("DEST_COUNTRY_NAME")).show(2)

    df.select(expr("*"), lit(1).as("One")).show(2)

    df.withColumn("numberOne", lit(1)).show(2)

    df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))

    df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns

    df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

    
  }
}
