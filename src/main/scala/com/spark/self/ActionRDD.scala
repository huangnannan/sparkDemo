package com.spark.self

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionRDD {
  def main(args: Array[String]): Unit = {
    //设置环境变量 local 代表本地环境，如果里面是*说明把所有的内存都用上
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建RDD
    val sc = new SparkContext(config)

    val RDD1: RDD[Int] = sc.makeRDD(1 to 10,2)
    //reduce 将所有元素聚合得到得结果
    val sum: Int = RDD1.reduce(_+_)
    println(sum)

    val RDD2: RDD[Int] = sc.makeRDD(Array(1,5,3,7,8,4,9))
    //对数据进行排序取前三个
    val RDD3 = RDD2.takeOrdered(3)
    RDD3.foreach(println)
  }

}
