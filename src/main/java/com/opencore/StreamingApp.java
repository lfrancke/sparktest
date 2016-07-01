package com.opencore;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class StreamingApp {

  public static void main(String... args) {
    SparkConf conf = new SparkConf();
    conf.setMaster("spark://localhost:7077");
    conf.setAppName("Spark Streaming Test Java");
    conf.set("spark.local.ip", "127.0.0.1");
    conf.setJars(new String[] {"target/sparktest-1.0-SNAPSHOT.jar"});

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(30));

    processStream(ssc, sc);

    ssc.start();
    ssc.awaitTermination();
  }

  private static void processStream(JavaStreamingContext ssc, JavaSparkContext sc) {
    System.out.println("--> Processing stream");
    JavaDStream<String> textStream = ssc.socketTextStream("localhost", 9999);
    textStream = textStream.repartition(4);

    int[] tick = {1};
    @SuppressWarnings("unchecked")
    JavaRDD<Integer>[] tickRdd = new JavaRDD[] {sc.parallelize(Collections.singletonList(tick[0]))};
    tickRdd[0].cache();
    tickRdd[0].count();

    textStream.foreachRDD(rdd -> {
      System.out.println("--> Starting new batch");
      System.out.println("Tick: " + tick[0]);
      if (tick[0] % 6 == 0) {
        tickRdd[0].unpersist();
        tickRdd[0] = sc.parallelize(Collections.singletonList(tick[0]));
        tickRdd[0].cache();
      }

      JavaPairRDD<Integer, Tuple2<String, Integer>> join = rdd.keyBy(String::length).join(tickRdd[0].keyBy(a -> a));

      join.foreachPartition(stringIterator -> {
        System.out.println("--> Working on new RDD");
        System.out.println("--> Current Tick is " + tick[0]);
        while (stringIterator.hasNext()) {
          Tuple2<Integer, Tuple2<String, Integer>> next = stringIterator.next();
          System.out.println("Working on -> " + next);
        }
      });

      tick[0]++;
    });
  }

}

