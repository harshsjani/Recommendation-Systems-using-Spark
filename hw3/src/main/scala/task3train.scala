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


object task3train {
  implicit val formats = DefaultFormats

  def genHashFns(numFuncs: Int): Array[(Long, Long)] = {
    val start = (10e7 - 5128423).toInt
    val end = (10e8 + 3267231).toInt
    val bstart = (10e7 - 3069127).toInt
    val bend = (10e8 + 1278127).toInt
    var fns = new ArrayBuffer[(Long, Long)]
    val random = new Random()

    for (i <- 0 to numFuncs) {
      val a = start + random.nextInt(end - start)
      val b = bstart + random.nextInt(bend - bstart)
      fns += Tuple2(a, b)
    }

    return fns.toArray
  }

  def genSignatures(uidValues: Set[Long], hashParams: Array[(Long, Long)], numBuckets: Int): Array[Long] = {
    val p = (10e9 + 7).toInt
    var sig = new ArrayBuffer[Long]

    for (params <- hashParams) {
      val a = params._1
      val b = params._2
      var sig_row = new ArrayBuffer[Long]

      for (v <- uidValues) {
        sig_row += (((a * v + b) % p) % numBuckets)
      }

      sig += sig_row.min
    }

    return sig.toArray
  }

  def loadRow(row: JValue): (String, (String, Double)) = {
    val s1 = (row\"user_id").extract[String]
    val t1 = (row\"business_id").extract[String]
    val t2 = (row\"stars").extract[Double]

    return (s1, (t1, t2))
  }

  def loadRowItem(row: JValue): (String, (String, Double)) = {
    val t1 = (row\"user_id").extract[String]
    val s1 = (row\"business_id").extract[String]
    val t2 = (row\"stars").extract[Double]

    return (s1, (t1, t2))
  }

  def genUBR(ubRDD: Array[(String, Array[(String, Double)])]): Map[String, HashMap[String, Double]] = {
    val ubr = new HashMap[String, HashMap[String, Double]]
    for (row <- ubRDD) {
      val tempMap = new HashMap[String, Double]
      for (x <- row._2) {
        tempMap(x._1) = x._2
      }
      ubr(row._1) = tempMap
    }
    return ubr.toMap
  }

  def genBUR(bur: scala.collection.Map[String, Array[(String, Double)]]): Map[String, HashMap[String, Double]] = {
    val ret = new HashMap[String, HashMap[String, Double]]

    for (item <- bur) {
      val bid = item._1
      val urs = item._2

      for (pair <- urs) {
        val u = pair._1
        val r = pair._2

        if (!ret.contains(bid)) {
          ret(bid) = new HashMap[String, Double]
        }
        ret(bid)(u) = r
      }
    }

    return ret.toMap
  }

  def pearsonSim(r1: HashMap[String, Double], r2: HashMap[String, Double]): Double = {
    val k1set = r1.keys.toSet
    val k2set = r2.keys.toSet
    val intsc = k1set.intersect(k2set)

    val c1 = new ArrayBuffer[Double]
    val c2 = new ArrayBuffer[Double]

    for (c <- intsc) {
      c1 += r1(c)
      c2 += r2(c)
    }

    val mean1 = c1.sum.toDouble / c1.size.toDouble
    val mean2 = c2.sum.toDouble / c2.size.toDouble

    val v1 = new ArrayBuffer[Double]
    val v2 = new ArrayBuffer[Double]

    for (r <- c1) {
      v1 += (r - mean1)
    }

    for (r <- c2) {
      v2 += (r - mean2)
    }

    var num = 0.0
    var den = 0.0

    for (i <- 0 to v1.size - 1) {
      val mult1 = v1(i)
      val mult2 = v2(i)
      val prod = mult1 * mult2
      num += prod
    }

    den = sqrt(v1.map(x => (x * x)).sum) * sqrt(v2.map(x => (x * x)).sum)
    if (num > 0 && den > 0) {
      return num / den
    }
    return 0
  }

  def writeData(simUsers: Array[(String, String, Double)], outputfile: String): Unit = {
    val out = new PrintWriter(new File(outputfile))

    for (item <- simUsers) {
      val output: Map[String, Any] = Map("u1"->item._1, "u2"->item._2, "sim"->item._3)
      val formatted_output = org.json4s.jackson.Serialization.write(output)
      out.write(formatted_output)
      out.write("\n")
    }

    out.close()
  }

  def writeDataItem(simBizz: Array[(String, String, Double)], outputfile: String): Unit = {
    val out = new PrintWriter(new File(outputfile))

    for (item <- simBizz) {
      val output: Map[String, Any] = Map("b1"->item._1, "b2"->item._2, "sim"->item._3)
      val formatted_output = org.json4s.jackson.Serialization.write(output)
      out.write(formatted_output)
      out.write("\n")
    }

    out.close()
  }

  def itemBased(args: Array[String]) = {
    val t1 = System.nanoTime

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    val trainfile = args(0)
    val modelfile = args(1)

    val textRDD = sc.textFile(trainfile)
      .map(row => parse(row))
      .map(row => loadRowItem(row))
      .groupByKey()
      .mapValues(values => values.toArray)
      .filter(row => row._2.size >= 3)
      .collectAsMap()

    val burMap = genBUR(textRDD)
    val uniqueBiz = burMap.keys

    val similarBizz = new ArrayBuffer[(String, String, Double)]
    for (item <- uniqueBiz.toArray.combinations(2)) {
      val rating1 = burMap(item(0))
      val rating2 = burMap(item(1))

      val intsc = rating1.keys.toSet.intersect(rating2.keys.toSet)
      if (intsc.size > 2) {
        val ps = pearsonSim(rating1, rating2)
        if (ps > 0)
          similarBizz += ((item(0), item(1), ps))
      }
    }

    writeDataItem(similarBizz.toArray, outputfile = modelfile)

    println("Duration: " + (System.nanoTime - t1) / 1e9d)
  }

  def userBased(args: Array[String]): Unit = {
    val t1 = System.nanoTime

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    val trainfile = args(0)
    val modelfile = args(1)
    val cfType = args(2)

    val NUM_BANDS = 50
    val NUM_HASHES = 50

    val textRDD = sc.textFile(trainfile).map(row => parse(row))

    val bizRDD = textRDD.map(row => (row\"business_id").extract[String]).distinct()
    val cmpMap = bizRDD.zipWithIndex().collectAsMap()
    val numBuckets = cmpMap.size - 1
    val hashParams = genHashFns(NUM_HASHES)

    val bizSets = textRDD.map(row => ((row \ "user_id").extract[String], cmpMap((row \ "business_id").extract[String])))
      .distinct()
      .groupByKey()
      .map(row => (row._1, row._2.toSet))
    bizSets.cache()
    val bizMap = bizSets.collectAsMap()

    println("Done generating bizSets and bizMap")

    val sigTemp = bizSets.mapValues(uidsList => genSignatures(uidsList, hashParams, numBuckets)).collect()

    println("Done generating sigTemp")

    var cands = new HashSet[(String, String)]()

    for (i <- 0 to NUM_BANDS - 1) {
      var curBucket = new HashMap[Long, HashSet[String]].withDefaultValue(HashSet[String]())
      for (row <- sigTemp) {
        if (curBucket.contains(row._2(i))) {
          curBucket(row._2(i)) += row._1
        } else {
          curBucket(row._2(i)) = new HashSet[String]
          curBucket(row._2(i)) += row._1
        }
      }
      for (v <- curBucket.values) {
        if (v.size > 1) {
          for (comb <- v.toArray.sorted.combinations(2)) {
            cands += Tuple2(comb(0), comb(1))
          }
        }
      }
    }

    println("Done generating Candidates")
    println(cands.size)

    val ubRDD = textRDD
      .map(row => loadRow(row))
      .groupByKey()
      .mapValues(x => x.toArray)
      .collect()

    val ubr = genUBR(ubRDD)

    var actualSimBizz = new ArrayBuffer[(String, String, Double)]
    for (pair <- cands) {
      val u1 = pair._1
      val u2 = pair._2

      val rating1 = ubr(u1)
      val rating2 = ubr(u2)

      val k1set = rating1.keys.toSet
      val k2set = rating2.keys.toSet

      val intsc = k1set.intersect(k2set)

      if (intsc.size >= 3) {
        val sim = intsc.size.toDouble / (k1set.union(k2set)).size.toDouble
        if (sim >= 0.01) {
          val ps = pearsonSim(rating1, rating2)
          if (ps > 0)
            actualSimBizz += Tuple3(u1, u2, ps)
        }
      }
    }

    println("Done generating similar pairs")
    println(actualSimBizz.size)

    writeData(actualSimBizz.toArray, outputfile = modelfile)

    println("Duration: " + (System.nanoTime - t1) / 1e9d)
  }

  def main(args: Array[String]) = {
    val cfType = args(2)

    if (cfType == "user_based")
      userBased(args)
    else
      itemBased(args)
  }
}
