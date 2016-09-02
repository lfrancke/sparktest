package com.opencore

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.functions._

object SparkSQLApp1 {

  def main(args: Array[String]): Unit = {
    val inputFile = "/Users/lars/Downloads/data-00007-of-00010"
    val conf = new SparkConf()
    conf.set("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext("local[*]", "testing", conf)

    val inputRdd = sc.textFile(inputFile)

    /*
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.text(inputFile)
    df.registerTempTable("readme")

    val sql = sqlContext.sql("SELECT length(value), COUNT(*) FROM readme GROUP BY length(value)")
    val rows = sql.collect()
    for (row <- rows) {
      println(row)
    }
    */


    val lengthRdd = inputRdd.map(_.length)
    val value = lengthRdd.countByValue()
    for (entry <- value) {
      println(entry)
    }

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.text(inputFile)
    val resultRdd = df.groupBy(length(col("value"))).count()
    val result = resultRdd.collect()
    for (row <- result) {
      println(row)
    }




    // Wait for keypress
    System.in.read()
  }

}
