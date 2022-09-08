package com.r2d2.scala.demo


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object App {
  case class Row(id: Int, name: String, surname: String, age: Int, city: String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TrackingChange")
    val sc = new SparkContext(conf)

    println("====DEMO DEPLOY====")

//    val rdd = sc.parallelize(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    val rdd_in_1 = sc.wholeTextFiles(s"file:///home/ec2-user/people_jan", 10);
    val rdd_in_2 = sc.wholeTextFiles(s"file:///home/ec2-user/people_feb", 10);

    val bal = rdd_in_1.map{path_data =>
      path_data.
//      val fields = row.
//      Movie(fields(0).toInt, fields(1), fields(2).toDouble)
    }

    //    val files = rdd.map(i => (s"/home/ec2-user/in/${i}", scala.io.Source.fromFile(s"/home/ec2-user/in/${i}").mkString))
//    val lines = rdd_in_1.flatMap(ok => ok._2.split("\n"))
//    val counts = lines.flatMap(line => line.split(" "))
//      .map(word => (word, 1))
//      .reduceByKey(_ + _, 10)

//    val formatted = counts.mapPartitionsWithIndex((index, iterator) => {
//      List((s"/home/ec2-user/out/${index}", iterator.map(wc => s"${wc._1}:${wc._2}\n").mkString)).iterator
//    })
//    val saved = formatted.foreach(pd => {

//      import java.io.File
//      import java.io.PrintWriter
//      def writeToFile(p: String, s: String): Unit = {
//        val pw = new PrintWriter(new File(p))
//        try pw.write(s) finally pw.close()
//      }
//
//      writeToFile(pd._1, pd._2)
//      println("done")

//    };)

//    sc.stop()
  }
}
