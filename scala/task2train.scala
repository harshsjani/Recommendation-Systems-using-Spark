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
      ret += ((x._1, 1))
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

    for (word: String <- wordsList.toSet) {
      val tfidf = tf(word) * idfMap(word)
      tfidfs += ((word, tfidf))
    }

    tfidfs.sortBy(x => (-x._2, x._1))

    tfidfs.take(200).map(x => x._1).toArray
  }

  def bizList2WordVec(bizList: Array[String], bizProfile: scala.collection.Map[String, Array[String]]): Array[String] = {
    var words = new HashSet[String]

    for (biz <- bizList) {
      words = words.union(bizProfile(biz).toSet)
    }
    return words.toArray.take(500)
  }

  def writeData(opf: String, bizProfile: scala.collection.Map[String, Array[String]], userProfile: scala.collection.Map[String, Array[String]]) = {
    val out = new PrintWriter(new File(opf))

    for (item <- bizProfile) {
      val output: Map[String, Any] = Map("type"->"biz", "biz_id"->item._1, "fvec"->item._2)
      val formatted_output = org.json4s.jackson.Serialization.write(output)
      out.write(formatted_output)
      out.write("\n")
    }

    for (item <- userProfile) {
      val output: Map[String, Any] = Map("type"->"user", "user_id"->item._1, "fvec"->item._2)
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

    val ipf = args(0)
    val opf = args(1)
    val stopwordsfile = args(2)

    val stopwords = sc.textFile(stopwordsfile).map(row => row.trim()).collect().toSet
    val textRDD = sc.textFile(ipf).map(row => parse(row))
    textRDD.persist(StorageLevel.DISK_ONLY)

    val biztextRDD = textRDD.map(row => ((row\"business_id").extract[String], (row\"text").extract[String]))
      .reduceByKey((x, y) => (x + " " + y))
      .mapValues(row => parseReviewList(row, stopwords))
    biztextRDD.persist(StorageLevel.DISK_ONLY)

    val bizCount = biztextRDD.count()

    val idfMap = biztextRDD.flatMap(row => idfFlatMap(row))
      .reduceByKey((x, y) => (x + y))
      .map(kv => (kv._1, log2(bizCount.toDouble / kv._2.toDouble)))
      .collectAsMap()

    println("Done creating IDF Map")

    val bizProfile = biztextRDD.mapValues(x => getBizProfile(x, idfMap)).collectAsMap()

    println("Done creating business profile")

    val userProfile = textRDD.map(row => ((row\"user_id").extract[String], ArrayBuffer[String]((row\"business_id").extract[String])))
      .reduceByKey((x, y) => x ++= y)
      .mapValues(value => value.toArray)
      .mapValues(value => bizList2WordVec(value, bizProfile))
      .collectAsMap()

    println("Done creating user profile")


    writeData(opf, bizProfile, userProfile)

    println("Done writing data")

    println("Duration: " + (System.nanoTime - t1) / 1e9d)
  }
}
