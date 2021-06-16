import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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


object task3predict {
  implicit val formats = DefaultFormats

  def sortIDs(id1: String, id2: String): (String, String) = {
    if (id2 < id1) {
      return (id2, id1)
    }
    return (id1, id2)
  }

  def loadItemRow(row: JValue): ((String, String), Double) = {
    var b1 = (row\"b1").extract[String]
    var b2 = (row\"b2").extract[String]
    val sim = (row\"sim").extract[Double]

    if (b2 < b1) {
      var temp = b1
      b1 = b2
      b2 = temp
    }

    return ((b1, b2), sim)
  }

  def ubrRow(row: JValue): (String, (String, Double)) = {
    var u1 = (row\"user_id").extract[String]
    var b1 = (row\"business_id").extract[String]
    val stars = (row\"stars").extract[Double]

    return (u1, (b1, stars))
  }

  def getItemPred(uid: String, bid: String, ubr: scala.collection.Map[String, Set[(String, Double)]], modelMap: scala.collection.Map[(String, String), Double]): Double = {
    val NEIGHBORS = 10

    var validNeighbors = new ArrayBuffer[(Double, Double)]

    if (!ubr.contains(uid)) {
      return 0.0
    }
    for (pair <- ubr(uid)) {
      val bid2 = pair._1
      val rating = pair._2
      val bids = sortIDs(bid2, bid)
      if (modelMap.contains(bids)) {
        validNeighbors += ((rating, modelMap(bids)))
      }
    }

    validNeighbors.sortBy(x => -x._2)
    validNeighbors = validNeighbors.take(NEIGHBORS)

    var top = 0.0
    var bot = 0.0

    for (x <- validNeighbors) {
      top += (x._1) * (x._2)
      bot += math.abs(x._2)
    }

    if (top == 0.0 || bot == 0.0) {
      return 0
    }
    return (top.toDouble / bot.toDouble)
  }

  def itemBased(args: Array[String]) = {
    val trainfile = args(0)
    val testfile = args(1)
    val modelfile = args(2)
    val outfile = args(3)

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    val modelMap = sc.textFile(modelfile)
      .map(row => parse(row))
      .map(row => loadItemRow(row))
      .collectAsMap()

    val ubr = sc.textFile(trainfile)
      .map(row => parse(row))
      .map(row => ubrRow(row))
      .groupByKey()
      .mapValues(value => value.toSet)
      .collectAsMap()

    val testPairs = sc.textFile(testfile)
      .map(row => parse(row))
      .map(row => ((row\"user_id").extract[String], (row\"business_id").extract[String]))
      .collect()

    val out = new PrintWriter(new File(outfile))

    for (pair <- testPairs) {
      val uid = pair._1
      val bid = pair._2
      val rating = getItemPred(uid, bid, ubr, modelMap)
      if (rating != 0) {
        val output: Map[String, Any] = Map("user_id"->uid, "business_id"->bid, "stars"->rating)
        val formatted_output = org.json4s.jackson.Serialization.write(output)
        out.write(formatted_output)
        out.write("\n")
      }
    }

    out.close()
  }

  def userBased(args: Array[String]) = {

  }

  def main(args: Array[String]) = {
    val t1 = System.nanoTime
    val cfType = args(4)

    if (cfType == "item_based") {
      itemBased(args)
    }
    else {
      userBased(args)
    }

    println("Duration: " + (System.nanoTime - t1) / 1e9d)
  }
}
