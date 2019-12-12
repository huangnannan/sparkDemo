package com.spark.self

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
    //创建sparkSession
    val sparkSession = new SparkSession(config)
    val frame = sparkSession.read.json("in/user.json")

  }

}
