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
  case class UserAction(userId: String, action: String)

  case class UserStatus(userId: String, active: Boolean)
  def updateUserStatus(userId: String,
                       newActions: Iterator[UserAction],
                       state: GroupState[UserStatus]): UserStatus = {
    val userStatus = state.getOption.getOrElse {
      new UserStatus(userId, false)
    }
    newActions.foreach { action =>
      userStatus.updateWith(action)
    }

    state.update(userStatus)
    return userStatus;
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ArbitraryStatefulComputationsExample")
      .getOrCreate()
    import spark.implicits._
    val userActions: Dataset[UserAction] = {}
    val latestStatuses = userActions
      .groupByKey(userAction => userAction.userId)
      .mapGroupsWithState(updateUserStatus _)
  }
}
