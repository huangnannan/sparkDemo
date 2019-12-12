package com.spark.self

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    //设置环境变量 local 代表本地环境，如果里面是*说明把所有的内存都用上
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建RDD
    val sc = new SparkContext(config)
    //按行读取文件
    val lines: RDD[String] = sc.textFile("in")
    //将每行映射成单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将每个单词映射成元祖
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //对每个元祖进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    // 选出key中包含sy的数据
    val filterRDD: RDD[(String, Int)] = wordToSum.filter(_._1.contains("sy"))
    //对数据进行排序 false 表示降序，默认升序
    val wordNumSort: RDD[(String, Int)] = filterRDD.sortBy(_._2,false)
    //收集数据
    val result: Array[(String, Int)] = wordNumSort.collect()
    //打印数据 (take(2) 表示取两条数据)
    result.take(2).foreach(println)


  }

  
}
