package com.jackpan.spark.chapter8
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._
/**
 *
 *
 * @author jackpan
 * @version v1.0 2021/10/27 13:28
 */
object ArbitraryStatefulComputationsExample {
  case class UserAction(userId: String, action: Boolean)

  case class UserStatus(userId: String, active: Boolean)
  def updateUserStatus(userId: String,
                       newActions: Iterator[UserAction],
                       state: GroupState[UserStatus]): UserStatus = {
    val userStatus = state.getOption.getOrElse {
      UserStatus(userId, active = false)
    }
    println(userStatus)
    newActions.foreach { action =>
      userStatus.copy(action.userId, action.action)
    }
    println("After...")
    println(userStatus)
    state.update(userStatus)
    userStatus
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ArbitraryStatefulComputationsExample")
      .getOrCreate()

//    val lines = spark
//      .readStream.format("socket") .option("host", "localhost") .option("port", 9999).load()
    import spark.implicits._
    val userActions: Dataset[UserAction] = Seq(UserAction("1", action = false), UserAction("2", action = true), UserAction("3", action = false),
      UserAction("4", action = true)).toDS()
    val latestStatuses = userActions
      .groupByKey(userAction => userAction.userId)
      .mapGroupsWithState(updateUserStatus _)

    latestStatuses.show()
  }
}
