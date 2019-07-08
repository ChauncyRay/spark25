package com.apple.day01

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsWithIndexHello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MPWI").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array(10,20,30,40,50,60))
    rdd.mapPartitionsWithIndex((index,items) => items.map((index,_))).collect.foreach(println)
    sc.stop
  }
}
