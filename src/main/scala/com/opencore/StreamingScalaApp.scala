package com.opencore

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingScalaApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    // Run it in Local mode
    //conf.setMaster("local[4]")

    // Run it in Standalone mode
    // Make sure to run mvn clean package first
    conf.setMaster("spark://localhost:7077")
    conf.setJars(List("target/sparktest-1.0-SNAPSHOT.jar"))
    conf.set("spark.local.ip", "127.0.0.1")

    conf.setAppName("Spark Streaming Test Scala")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(30))

    // You can start a server using: nc -lk 9999
    var stream: DStream[String] = ssc.socketTextStream("localhost", 9999)
    stream = stream.repartition(4)

    var tick = 1
    var tickRdd = sc.parallelize(List(tick))

    stream.foreachRDD(rdd => {
      println("--> Starting new batch")
      println(s"Tick: $tick")
      if (tick % 6 == 0) {
        tickRdd.unpersist()
        tickRdd = sc.parallelize(List(tick))
        tickRdd.cache()
        println(tickRdd.count())
      }

      val join: RDD[(Int, (String, Int))] = rdd.keyBy(_.length).join(tickRdd.keyBy(a => a))
      join.foreachPartition(iterator => {
        println("--> Working on new RDD")
        println(s"--> Current Tick is $tick")
        for (next <- iterator) {
          println(s"Working on -> $next")
        }
      })

      tick += 1
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
