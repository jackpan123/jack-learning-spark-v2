package com.jackpan.spark.chapter10

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.RegressionEvaluator
object DecisionTreeExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("BaselineModelExample")
      .getOrCreate()

    val filePath = "/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/sf-airbnb/sf-airbnb-clean.parquet"
    val airbnbDF = spark.read.parquet(filePath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms", "bathrooms",
      "number_of_reviews", "price").show(5)

    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed = 42)

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")
    val oheOutputCols = categoricalCols.map(_ + "OHE")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val dt = new DecisionTreeRegressor()
      .setLabelCol("price")

    val numericCols = trainDF.dtypes.filter{ case (field, dataType) =>
      dataType == "DoubleType" && field != "price"}.map(_._1)

    val assemblerInputs = indexOutputCols ++ numericCols
    val vectorAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    val stages = Array(stringIndexer, vectorAssembler, dt)
    val pipeline = new Pipeline()
      .setStages(stages)

    dt.setMaxBins(40)
    val pipelineModel = pipeline.fit(trainDF)

    val dtModel = pipelineModel.stages.last
      .asInstanceOf[org.apache.spark.ml.regression.DecisionTreeRegressionModel]
    println(dtModel.toDebugString)

    // 提取特征的重要性
    val featureImp = vectorAssembler
      .getInputCols.zip(dtModel.featureImportances.toArray)
    val columns = Array("feature", "Importance")
    val featureImpDF = spark.createDataFrame(featureImp).toDF(columns: _*)
    featureImpDF.orderBy(col("Importance").desc).show()
    val predDF = pipelineModel.transform(testDF)
    predDF.select("features", "price", "prediction").orderBy(desc("price")).show(10)

    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")
      .setMetricName("rmse")

    val rmse = regressionEvaluator.evaluate(predDF)
    val r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)

    println(s"RMSE is $rmse")
    println(s"R2 is $r2")
    println("*-"*80)
  }
}
