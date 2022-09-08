package com.r2d2.scala.demo


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.collection.TraversableOnce

object App {
  case class Record(id: Int, name: String, surname: String, age: Int, city: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TrackingChange")
    val sc = new SparkContext(conf)

    val jan = s"file:///home/ec2-user/people_jan"
    val feb = s"file:///home/ec2-user/people_feb"
    val deserialize: ((String, String)) => TraversableOnce[Record] = tup => {
       tup._2.split("\n") .map((row: String) => {
        val fields = row.split(",").map(_.trim)
        Record(fields(0).toInt, fields(1), fields(2), fields(3).toInt, fields(4))
      })
    }
    val rdd1 = sc.wholeTextFiles(jan, 10)
      .flatMap(deserialize)
      .map(rec => { (rec.id, rec)})
    val rdd2 = sc.wholeTextFiles(feb, 10)
      .flatMap(deserialize)
      .map(rec => { (rec.id, rec)})

    val all = rdd1.join(rdd2)
    val formatted = all.mapPartitionsWithIndex((index, iterator) => {
      List((
        s"/home/ec2-user/out/$index",
        iterator.map(recs => {
          val old = recs._2._1
          val cur = recs._2._2
          if (old.city != cur.city) {
            s"${old.id},${old.name},${old.surname},${old.age},${old.city}\n${cur.id},${cur.name},${cur.surname},${cur.age},${cur.city}\n"
          } else {
            s"${old.id},${old.name},${old.surname},${old.age},${old.city}\n"
          }
        }).mkString
      )).iterator
    })

    formatted.foreach(pd => {
      import java.io.File
      import java.io.PrintWriter
      def writeToFile(p: String, s: String): Unit = {
        val pw = new PrintWriter(new File(p))
        try pw.write(s) finally pw.close()
      }

      writeToFile(pd._1, pd._2)
      println("done")

    })

    sc.stop()
  }
}
