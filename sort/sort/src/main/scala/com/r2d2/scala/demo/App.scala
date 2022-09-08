package com.r2d2.scala.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.TraversableOnce

object App {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sort")
    val sc = new SparkContext(conf)

    val dir_name = s"file:///home/ec2-user/in"
    val deserialize: ((String, String)) => TraversableOnce[Int] = tup => {
      tup._2.split("\n").map((num: String) => {
        num.trim().toInt
      })
    }
    sc.wholeTextFiles(dir_name, 10)
      .flatMap(deserialize)
      .sortBy(k => k, ascending = true)
      .collect()

    sc.stop()
  }
}
