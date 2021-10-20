package com.jackpan.spark.chapter7

import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/20 21:59
 */
object SortMergeJoinExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SortMergeJoinExample")
      .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    // Initialize states and items purchased
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4",
      5-> "SKU-5")

    import spark.implicits._
    // Create DataFrames
    val usersDF = (0 to 100000).map(id => (id, s"user_${id}",
      s"user_${id}@databricks.com", states(rnd.nextInt(5))))
      .toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 100000)
      .map(r => (r, r, rnd.nextInt(1000), 10 * r* 0.2d,
        states(rnd.nextInt(5)), items(rnd.nextInt(5))))
      .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

    usersOrdersDF.show(false)

    usersOrdersDF.explain()

    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .bucketBy(8, "uid")
      .mode(SaveMode.Overwrite)
      .saveAsTable("UsersTbl")

    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")

    // Read them back in
    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    // Do the join and show the results
    val joinUsersOrdersBucketDF = ordersBucketDF
      .join(usersBucketDF, $"users_id" === $"uid")

    joinUsersOrdersBucketDF.show(false)
    joinUsersOrdersBucketDF.explain()
  }
}
