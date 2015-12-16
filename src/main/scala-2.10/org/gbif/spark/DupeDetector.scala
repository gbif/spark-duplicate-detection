package org.gbif.spark

import grizzled.slf4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext._
import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer

object DupeDetector {
  val logger = Logger[this.type]

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Duplicate Detection")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // paths refer to hdfs
    val df = sqlContext.read.parquet("/user/oliver/hdfs_export/occurrence_parquet_full")
    df.cache()

    // generate a new df with gbifid | fingerprint, then make fingerprint the key and reducebykey and make result a concat of gbifids
    val matches = df.map(row => {
      val compId = row.get(row.fieldIndex("gbifid")).toString
      val compDataset = if (row.isNullAt(row.fieldIndex("datasetkey"))) None: Option[String] else Option[String](row.getString(row.fieldIndex("datasetkey")))
      val compLat = if (row.isNullAt(row.fieldIndex("lat"))) None: Option[Double] else Option[Double](row.getDouble(row.fieldIndex("lat")))
      val compLng = if (row.isNullAt(row.fieldIndex("lng"))) None: Option[Double] else Option[Double](row.getDouble(row.fieldIndex("lng")))
      val compDate = if (row.isNullAt(row.fieldIndex("lng"))) None: Option[Long] else Option[Long](row.getLong(row.fieldIndex("date")))
      val compCollector = if (row.isNullAt(row.fieldIndex("collector"))) None: Option[String] else Option[String](row.getString(row.fieldIndex("collector")))
      val compSpeciesKey = row.getInt(row.fieldIndex("taxonkey"))
      (generateFingerprint(compSpeciesKey, compLat, compLng, compDate, compCollector), compId + '|' + compDataset.getOrElse("").substring(0, 7))
    }).repartition(600) // lots of small chunks so we don't get a few huge chunks that take a really long time on a single thread
      .persist // cache in memory for the duration of this job
      .reduceByKey((x, y) => x + " " + y) // group by the fingerprint, where the "values" are concatenated to produce our dupes
      .filter(t => t._2.contains(" ")) // keep only those rows where dupe count > 1
      .filter(t => {
        val ids = t._2.split(" ")
        var sameDataset = true
        val firstDataset = ids(0).substring(ids(0).indexOf("|") + 1)
        ids.foreach(s => {
          sameDataset = sameDataset && s.substring(s.indexOf("|")+1) == firstDataset
        })
        // filter says "keep only those that return true for condition" and we want only those where datasets are not all the same
        !sameDataset
    })

    matches.saveAsTextFile("/user/oliver/spark-duplicate-detector/" + System.currentTimeMillis().toString)
  }

  // a simple fingerpint of a "row" that just concatenates the important fields
  def generateFingerprint(specieskey: Int, lat: Option[Double], lng: Option[Double], date: Option[Long], collector: Option[String]): String = {
    val latString = if (lat.isDefined) lat.get.toString else ""
    val lngString = if (lng.isDefined) lng.get.toString else ""
    val dateString = if (date.isDefined) date.get.toString else ""
    specieskey.toString + '|' + latString + '|' + lngString + '|' + dateString + '|' + collector.getOrElse("")
  }
}
