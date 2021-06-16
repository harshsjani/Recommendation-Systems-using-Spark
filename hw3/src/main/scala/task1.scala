import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.control.Breaks._
import scala.util.Random._
import collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.io.{File, PrintWriter}
import scala.io.Source
import scala.math.Ordering.Implicits._
import scala.util.Random


object task1 {
  implicit val formats = DefaultFormats

  def genHashFns(numFuncs: Int): Array[(Int, Int)] = {
    val step = 10e7.toInt
    var curstep = step
    val start = (10e9 - 10e8 + 17).toInt
    val bstart = (10e9 - 10e8 + 30697).toInt
    var fns = new ArrayBuffer[(Int, Int)]
    val random = new Random()

    for (i <- 0 to numFuncs) {
      val a = start + random.nextInt(curstep - 10e7.toInt + 23)
      val b = bstart + random.nextInt((10e9 + curstep + 10e8 - 4123892 - bstart).toInt)
      curstep += step
      fns += Tuple2(a, b)
    }

    return fns.toArray
  }

  def genSignatures(uidValues: Set[Long], hashParams: Array[(Int, Int)], numBuckets: Int): Array[Long] = {
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

  def writeData(simBizz: Array[(String, String, Double)], outputfile: String): Unit = {
    val out = new PrintWriter(new File(outputfile))

    for (item <- simBizz) {
      val output: Map[String, Any] = Map("b1"->item._1, "b2"->item._2, "sim"->item._3)
      val formatted_output = org.json4s.jackson.Serialization.write(output)
      out.write(formatted_output)
    }

    out.close()
  }

  def main(args: Array[String]) = {
    val ipf = args(0)
    val opf = args(1)

    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    val NUM_BANDS = 100
    val NUM_HASHES = 100

    val textRDD = sc.textFile(ipf).map(row => parse(row))
    textRDD.cache()

    val userRDD = textRDD.map(row => (row \ "user_id").extract[String]).distinct()
    val cmp_map = userRDD.zipWithIndex().collectAsMap()
    val num_buckets = cmp_map.size - 1
    val hash_params = genHashFns(NUM_HASHES)

    val bizSets = textRDD.map(row => ((row \ "business_id").extract[String], cmp_map((row \ "user_id").extract[String])))
      .distinct()
      .groupByKey()
      .map(row => (row._1, row._2.toSet))
    bizSets.cache()
    val bizMap = bizSets.collectAsMap()

    val sigTemp = bizSets.mapValues(uidsList => genSignatures(uidsList, hash_params, num_buckets)).collect()

    var cands = new HashSet[(String, String)]()

    for (i <- 0 to NUM_BANDS) {
      var curBucket = new HashMap[Long, HashSet[String]].withDefaultValue(new HashSet[String]())
      for (row <- sigTemp) {
        curBucket(row._2(i)) += row._1
      }
      for (v <- curBucket.values) {
        if (v.size > 1) {
          for (comb <- v.toArray.sorted.combinations(2)) {
            cands += Tuple2(comb(0), comb(1))
          }
        }
      }
    }

    var actualSimBizz = new ArrayBuffer[(String, String, Double)]
    for (pair <- cands) {
      val b1 = pair._1
      val b2 = pair._2

      val u1 = bizMap(b1)
      val u2 = bizMap(b2)

      val sim = (u1.intersect(u2)).size.toDouble / (u1.union(u2)).size.toDouble
      if (sim >= 0.05) {
        actualSimBizz += Tuple3(b1, b2, sim)
      }
    }

    writeData(actualSimBizz.toArray, outputfile = opf)
  }
}
