package com.r2d2.scala.demo


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math._

object App {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    println("====DEMO DEPLOY====")

    val data_1 = List(1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024);
    val rdd_1 = sc.parallelize(data_1, 4);
    var a_init = rdd_1.flatMap((x: Int) => {Vector.fill(x)(x)});

    val data_2 = List(512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512, 512);
    val rdd_2 = sc.parallelize(data_2, 4);
    var b_init = rdd_2.flatMap((x: Int) => {Vector.fill(x)(x)});

    for(i <- 0 until 10){
      val a_1 = a_init.map((x: Int) => x*2)
      val b_1 = b_init.map((x: Int) => x*20)

      val a_2 = a_1.map((x: Int) => sin(x))
      val b_2 = b_1.map((x: Int) => cos(x))

      val a_3 = a_2.filter((x: Double) => x < 0.5)
      val b_3 = b_2.filter((x: Double) => x >= 0.5)

      val a_4 = a_3.map((x: Double) => {((x * 1000.0).floor.toInt, x)})
      val b_4 = b_3.map((x: Double) => {((x * 1000.0).floor.toInt, x)})

      val all = a_4.join(b_4, 8)

      val a_5 = a_4.filter((x) => x._1 >= 500)
      val b_5 = b_4.filter((x) => x._1 < 500)

      a_init = a_5.map((x) => x._1)
      b_init = b_5.map((x) => x._1)

    }

    a_init.collect()
    b_init.collect()

//    val rdd = sc.parallelize(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
//    val data = sc.wholeTextFiles(s"file:///home/ec2-user/in/", 10);
//    val files = rdd.map(i => (s"/home/ec2-user/in/${i}", scala.io.Source.fromFile(s"/home/ec2-user/in/${i}").mkString))
//    val lines = files.flatMap(ok => ok._2.split("\n"))
//    val counts = lines.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _, 10)
//
//    val formatted = counts.mapPartitionsWithIndex((index, iterator) => {
//      List((s"/home/ec2-user/out/${index}", iterator.map(wc => s"${wc._1}:${wc._2}\n").mkString)).iterator
//    })
//    val saved = formatted.foreach(pd => {
//
//      import java.io.File
//      import java.io.PrintWriter
//      def writeToFile(p: String, s: String): Unit = {
//        val pw = new PrintWriter(new File(p))
//        try pw.write(s) finally pw.close()
//      }
//
//      writeToFile(pd._1, pd._2)
//      println("done")
//
//    })
//
//    sc.stop()
  }
}
