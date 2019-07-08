package com.apple.day01

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapHello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlatMap").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array(1,2,3,4,5))
    rdd.flatMap(x => Array(x * x,x * x * x)).collect.foreach(println)
    sc.stop
  }
}
