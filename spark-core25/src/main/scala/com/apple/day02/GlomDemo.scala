package com.apple.day02

import org.apache.spark.{SparkConf, SparkContext}

object GlomDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GlomDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10)
    val result = rdd.glom()
    result.collect.mkString.foreach(print)
    sc.stop
  }
}
