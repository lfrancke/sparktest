package com.opencore

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple Scala application that does a simple count of the length of all words in a file.
  *
  * It uses three different APIs for this task: RDD, DataFrame (using SQL and a pure version) and Dataset
  */
object ScalaApp {

  def main(args: Array[String]): Unit = {
    val logFile = "README.md"
    val sc = new SparkContext("local[*]", "testing", new SparkConf())

    // RDD
    val readmeRdd = sc.textFile(logFile)
    val lengthRdd = readmeRdd.map(_.length)
    var combinedLength = lengthRdd.reduce(_ + _)
    println(combinedLength)

    // DataFrame: Setup
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.text(logFile)
    df.registerTempTable("readme")

    // DataFrame: SQL
    val sql = sqlContext.sql("SELECT SUM(length(value)) FROM readme")
    val combinedLengthLong = sql.first().getLong(0)
    println(combinedLengthLong)

    // DataFrame
    val lengthRdd2 = df.map(_.getString(0).length)
    combinedLength = lengthRdd2.reduce(_ + _)
    println(combinedLength)

    // Dataset
    import sqlContext.implicits._
    val dataset = sqlContext.createDataset(readmeRdd)
    val lengthDataset = dataset.map(_.length)
    combinedLength = lengthDataset.reduce(_ + _)
    println(combinedLength)
  }

}
