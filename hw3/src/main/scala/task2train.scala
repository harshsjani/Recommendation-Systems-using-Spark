import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.math.log10
import scala.util.control.Breaks._
import scala.util.Random._
import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.io.{File, PrintWriter}
import scala.io.Source
import scala.math.Ordering.Implicits._
import scala.util.Random


object task2train {
  implicit val formats = DefaultFormats

  def parseReviewList(row: String, stopwords: Set[String]): Tuple2[Array[String], Map[String, Double]] = {
    val row2 = row.toLowerCase()
    val chars = new ArrayBuffer[Char]

    for (c <- row2) {
      if (c.isLetter || c == ' ') {
        chars += c
      }
    }
    val row3 = chars.mkString("")

    val validwords = new ArrayBuffer[String]
    for (word <- row3.split(" ")) {
      if (!stopwords.contains(word)) {
        validwords += word
      }
    }
    val ctr = validwords.groupBy(identity).mapValues(_.size)
    val highest_wc = ctr.values.max
    var tf = new HashMap[String, Double]

    for (pair <- ctr) {
      tf(pair._1) = pair._2.toDouble / highest_wc.toDouble
    }

    return (validwords.toArray, tf.toMap)
  }

  def idfFlatMap(row: Tuple2[String, Tuple2[Array[String], Map[String, Double]]]): TraversableOnce[(String, Int)] = {
    val ret = new ArrayBuffer[(String, Int)]

    val ctr = row._2._1.groupBy(identity).mapValues(_.size)

    for (x <- ctr) {
      ret += (x._1, 1)
    }

    return ret.toArray.toTraversable
  }

  def log2(x: Double): Double = {
    log10(x)/log10(2.0)
  }

  def getBizProfile(row: (Array[String], Map[String, Double]), idfMap: scala.collection.Map[String, Double]): Array[String] = {
    val wordsList = row._1
    val tf = row._2

    val tfidfs = new ArrayBuffer[(String, Double)]

    for (word <- wordsList.toSet) {
      val tfidf = tf(word) * idfMap(word)
      tfidfs += (word, tfidf)
    }

    tfidfs.sortBy(x => (-x._2, x._1))

    tfidfs.take(200).map(x => x._1).toArray
  }

  def main(args: Array[String]) = {
    val t1 = System.nanoTime

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    val ipf = args(0)
    val opf = args(1)
    val stopwordsfile = args(2)

    val stopwords = sc.textFile(stopwordsfile).map(row => row.trim()).collect().toSet
    val textRDD = sc.textFile(ipf).map(row => parse(row))
    textRDD.cache()

    val biztextRDD = textRDD.map(row => ((row\"business_id").extract[String], (row\"text").extract[String]))
      .reduceByKey((x, y) => (x + " " + y))
      .mapValues(row => parseReviewList(row, stopwords))

    biztextRDD.cache()

    val bizCount = biztextRDD.count()

    val idfMap = biztextRDD.flatMap(row => idfFlatMap(row))
      .reduceByKey((x, y) => (x + y))
      .map(kv => (kv._1, log2(bizCount.toDouble / kv._2.toDouble)))
      .collectAsMap()

    val bizProfile = biztextRDD.mapValues(x => getBizProfile(x, idfMap))
    biztextRDD.unpersist()

    val userProfile = textRDD.map(row => )

    println("Duration: " + (System.nanoTime - t1) / 1e9d)
  }
}
