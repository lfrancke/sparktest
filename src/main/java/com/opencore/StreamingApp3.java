package com.opencore;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class StreamingApp3 {

  public static void main(String... args) {
    SparkConf conf = new SparkConf();
    conf.setAppName("Spark Streaming Test Java");

    conf.setMaster("local[*]");
    conf.set("spark.streaming.concurrentJobs", "2");

    /*
    conf.setMaster("spark://localhost:7077");
    conf.set("spark.local.ip", "127.0.0.1");
    conf.setJars(new String[] {"target/sparktest-1.0-SNAPSHOT.jar"});
    */

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

    processStream(ssc, sc);

    ssc.start();
    ssc.awaitTermination();
  }

  private static void processStream(JavaStreamingContext ssc, JavaSparkContext sc) {
    System.out.println("--> Processing stream");

    JavaRDD<String> inputRdd = sc.parallelize(Collections.singletonList("hello world")).map(v1 -> {
      System.out.println("RDD called");
      return v1;
    });

    Queue<JavaRDD<String>> queue = new LinkedList<>();
    queue.add(inputRdd);
    ssc.remember(Durations.seconds(0));
    JavaDStream<String> stream = ssc.queueStream(queue, false, inputRdd).repartition(1);

    stream.foreachRDD(rdd -> {
      rdd.foreachPartition(stringIterator -> {
        System.out.println("Stream 1 - Pre Sleep");
        Thread.sleep(10000);
        System.out.println("Stream 1 - Post Sleep");
        while (stringIterator.hasNext()) {
          String next = stringIterator.next();
          System.out.println(next);
        }
      });
    });

    stream.foreachRDD(rdd -> {
      rdd.foreachPartition(stringIterator -> {
        System.out.println("Stream 2");
        //Thread.sleep(3000);
        while (stringIterator.hasNext()) {
          String next = stringIterator.next();
          System.out.println(next);
        }
      });
    });

  }

}

