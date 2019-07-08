package com.apple.day01

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val result = sc.makeRDD(1 to 10).mapPartitions(x => x.map(_ * 2))
    result.collect.foreach(println)
    sc.stop
  }
}
