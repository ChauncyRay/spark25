package com.apple.day01

import org.apache.spark.{SparkConf, SparkContext}

object MapHello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Map").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(1 to 10)
    val result = rdd1.map(_ * 2).collect
    result.foreach(println)
    sc.stop
  }
}
