package com.jackpan.spark.chapter7
import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/19 20:09
 */
object SparkConfigurationExample {

  def printConfigs(session: SparkSession): Unit = {
    val mconf = session.conf.getAll
    for (k <- mconf.keySet) {
      println(s"${k} -> ${mconf(k)}\n")
    }
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .master("local[*]")
      .appName("SparkConfigurationExample")
      .getOrCreate()

    printConfigs(spark)

    spark.conf.set("spark.sql.shuffle.partitions",
      spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)
  }
}
