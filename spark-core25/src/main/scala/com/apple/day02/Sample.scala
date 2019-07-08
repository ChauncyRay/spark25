package com.apple.day02

import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GlomDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(30,50,70,60,10,20))
    //抽样
    val rdd2 = rdd.sample(false,0.5)
    println(rdd2.collect.toList)


    sc.stop
  }
}
