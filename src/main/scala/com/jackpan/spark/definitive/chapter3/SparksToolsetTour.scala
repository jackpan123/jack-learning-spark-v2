package com.jackpan.spark.definitive.chapter3

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering.KMeans


case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

object SparksToolsetTour {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparksToolsetTour")
      .getOrCreate()

    import spark.implicits._
    val flightsDF = spark.read.parquet("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/flight_data/parquet/2010-summary.parquet")

    val flights = flightsDF.as[Flight]


    flights
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(flight_row => flight_row)
      .take(5)

    flights.take(5)
      .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
      .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

    val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/retail-data/by-day/*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema

    staticDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load("/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/retail-data/by-day/*.csv")

    println(streamingDataFrame.isStreaming)

    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        $"CustomerId", window($"InvoiceDate", "1 day"))
      .sum("total_cost")

//    purchaseByCustomerPerHour.writeStream
//      .format("memory")
//      .queryName("customer_purchases")
//      .outputMode("complete")
//      .start()

//    spark.sql(
//      """
//        SELECT *
//        FROM customer_purchases
//        ORDER_BY 'sum(total_cost)' DESC
//        """).show(5)
//    spark.sql(
//      """
//        SELECT *
//        FROM customer_purchases
//        ORDER BY `sum(total_cost)` DESC
//        """)
//      .show(5)


//    purchaseByCustomerPerHour.writeStream
//      .format("console")
//      .queryName("customer_purchases_2")
//      .outputMode("complete")
//      .start()

    staticDataFrame.printSchema()

    val preppedDataFrame = staticDataFrame
      .na.fill(0)
      .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
      .coalesce(5)

    val trainDataFrame = preppedDataFrame
      .where("InvoiceDate < '2011-07-01'")

    val testDataFrame = preppedDataFrame
      .where("InvoiceDate >= '2011-07-01'")

    println(trainDataFrame.count())
    println(testDataFrame.count())

    val indexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_of_week_index")

    val encoder = new OneHotEncoder()
      .setInputCol("day_of_week_index")
      .setOutputCol("day_of_week_encoded")

    val VectorAssembler = new VectorAssembler()
      .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
      .setOutputCol("features")

    val transformationPipeline = new Pipeline()
      .setStages(Array(indexer, encoder, VectorAssembler))

    val fittedPipeline = transformationPipeline.fit(trainDataFrame)

    val transformedTraining = fittedPipeline.transform(trainDataFrame)

    transformedTraining.cache()

    val kmeans = new KMeans()
      .setK(20)
      .setSeed(1L)

    val kmModel = kmeans.fit(transformedTraining)

//    kmModel.c

  }
}
