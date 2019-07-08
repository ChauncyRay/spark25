package com.apple.day01

import org.apache.spark.{SparkConf, SparkContext}

object SparkHello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val resultRDD = lines.flatMap(_.split("\\W+")).map((_,1)).reduceByKey(_+_)
    val result = resultRDD.collect
    result.foreach(println)
    sc.stop()


  }
}
