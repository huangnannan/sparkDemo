package com.spark.self

import org.apache.spark.{SparkConf, SparkContext}

object RDDOperator {


  def union(sc: SparkContext): Unit = {
    //创建第一个集合
    val RDD1 = sc.parallelize(1 to 5)
    //创建第二个集合
    val RDD2 = sc.parallelize(5 to 10)
    //并集
    val RDD3 = RDD1.union(RDD2).collect()
    RDD3.foreach(println)
  }


  def intersection(sc: SparkContext): Unit = {
    //创建第一个集合
    val RDD1 = sc.parallelize(1 to 6)
    //创建第二个集合
    val RDD2 = sc.parallelize(4 to 10)
    //交集
    val RDD3 = RDD1.intersection(RDD2).collect()

    RDD3.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    //设置环境变量 local 代表本地环境，如果里面是*说明把所有的内存都用上
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建RDD
    val sc = new SparkContext(config)
    // 求两个集合得并集
//    union(sc)
    // 求两个集合得交集
    intersection(sc)
  }

}
