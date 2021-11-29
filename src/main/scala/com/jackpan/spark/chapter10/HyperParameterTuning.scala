package com.jackpan.spark.chapter10

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.CrossValidator


object HyperParameterTuning {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HyperParameterTuning")
      .getOrCreate()

    val filePath = "/Users/jackpan/JackPanDocuments/jack-project/spark/jack-learning-spark-v2/data/sf-airbnb/sf-airbnb-clean.parquet"

    val airbnbDF = spark.read.parquet(filePath)
    val Array(trainDF, testDF) = airbnbDF.randomSplit(Array(.8, .2), seed=42)

    val categoricalCols = trainDF.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")

    val stringIndexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(indexOutputCols)
      .setHandleInvalid("skip")

    val numericCols = trainDF.dtypes.filter{ case (field, dataType) => dataType == "DoubleType" && field != "price"}.map(_._1)
    val assemblerInputs = indexOutputCols ++ numericCols
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    val rf = new RandomForestRegressor()
      .setLabelCol("price")
      .setMaxBins(40)
      .setSeed(42)



    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(2, 4, 6))
      .addGrid(rf.numTrees, Array(10, 100))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val cv = new CrossValidator()
      .setEstimator(rf)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setParallelism(4)
      .setNumFolds(3)
      .setSeed(42)

//    val cvModel = cv.setParallelism(4).fit(trainDF)
//
//    cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics)
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vecAssembler, cv))
    val pipelineModel = pipeline.fit(trainDF)


  }
}
