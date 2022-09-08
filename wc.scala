val rdd = sc.parallelize(Seq(0,1))
val files = rdd.map(i => (s"./in/${i}", scala.io.Source.fromFile(s"./in/${i}").mkString))
val lines = files.flatMap(ok => ok._2.split("\n"))
val counts = lines.flatMap(line => line.split(" "))
                    .map(word => (word,1))
                    .reduceByKey(_+_)

val formatted = counts.mapPartitionsWithIndex((index, iterator) => {
List((s"./out/${index}", iterator.map( wc => s"${wc._1}:${wc._2}\n" ).mkString)).iterator
})
val saved = formatted.foreach(pd => {

import java.io.File
import java.io.PrintWriter
def writeToFile(p: String, s: String): Unit = {
  val pw = new PrintWriter(new File(p))
  try pw.write(s) finally pw.close()
}
writeToFile(pd._1, pd._2)


})
