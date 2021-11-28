package com.jackpan.spark.chapter10

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder,StringIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator


object MachineLearningExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MachineLearningExample")
      .getOrCreate()

    val filePath = "/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
      "number_of_reviews", "price").show(5)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)
    println(f"""There are ${trainDF.count} rows in the training set, and ${testDF.count} in the test set""")

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")
    val oheOutputCols = categoricalCols.map(_ + "OHE")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val oheEncoder = new OneHotEncoder()
      .setInputCols(indexOutputCols)
      .setOutputCols(oheOutputCols)

    val numericCols = trainDF.dtypes.filter{ case (field, dataType) =>
      dataType == "DoubleType" && field != "price"}.map(_._1)

    val assemblerInputs = oheOutputCols ++ numericCols
    val vectorAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("price")

    val pipeline = new Pipeline().setStages(Array(stringIndexer, oheEncoder, vectorAssembler, lr))
    val pipelineModel = pipeline.fit(trainDF)
    val predDF = pipelineModel.transform(testDF)
    predDF.select("features", "price", "prediction").show(5, truncate = false)

    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator.evaluate(predDF)
    println(f"RMSE is $rmse%.1f")

    val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    println(s"R2 is $r2")

    val pipelinePath = "/Users/jackpan/JackPanDocuments/temporary/model/lr-pipeline-model"
    pipelineModel.write.overwrite().save(pipelinePath)
  }
}
