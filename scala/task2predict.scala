import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.math.log10
import scala.util.control.Breaks._
import scala.util.Random._
import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.io.{File, PrintWriter}
import scala.io.Source
import scala.math.Ordering.Implicits._
import scala.math.sqrt
import scala.util.Random


object task2predict {
  implicit val formats = DefaultFormats

  def cosineSim(userVec: Set[String], bizVec: Set[String]): Double = {
    if (userVec.size == 0)
      return 0
    if (bizVec.size == 0)
      return 0

    val num = userVec.intersect(bizVec).size
    val den = sqrt(userVec.size) * sqrt(bizVec.size)

    return num.toDouble / den.toDouble
  }

  def writeData(opf: String, data: Array[(String, String, Double)]) = {
    val out = new PrintWriter(new File(opf))

    for (item <- data) {
      val output: Map[String, Any] = Map("user_id"->item._1, "business_id"->item._2, "sim"->item._3)
      val formatted_output = org.json4s.jackson.Serialization.write(output)
      out.write(formatted_output)
      out.write("\n")
    }

    out.close()
  }

  def main(args: Array[String]) = {
    val t1 = System.nanoTime

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    val testfile = args(0)
    val modelfile = args(1)
    val outputfile = args(2)

    val modelRDD = sc.textFile(modelfile).map(row => parse(row))

    val uzp = modelRDD.filter(row => (row\"type").extract[String] == "user")
      .map(row => ((row\"user_id").extract[String], (row\"fvec").extract[Array[String]]))
      .mapValues(value => value.toSet)
      .collectAsMap()

    val bzp = modelRDD.filter(row => (row\"type").extract[String] == "biz")
      .map(row => ((row\"biz_id").extract[String], (row\"fvec").extract[Array[String]]))
      .mapValues(value => value.toSet)
      .collectAsMap()

    val ans = sc.textFile(testfile)
      .map(row => parse(row))
      .map(row => ((row\"user_id").extract[String], (row\"business_id").extract[String]))
      .map(row => (row._1, row._2, cosineSim(uzp(row._1), bzp(row._2))))
      .filter(row => row._3 >= 0.01)
      .collect()

    writeData(outputfile, ans)

    println("Duration: " + (System.nanoTime - t1) / 1e9d)
  }
}
