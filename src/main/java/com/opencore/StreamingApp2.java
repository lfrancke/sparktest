package com.opencore;

import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class StreamingApp2 {

  public static void main(String... args) {
    SparkConf conf = new SparkConf();
    conf.setMaster("local[2]");
    conf.setAppName("Spark Streaming Test Java");

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(30));

    processStream(ssc, sc);

    ssc.start();
    ssc.awaitTermination();
  }

  private static void processStream(JavaStreamingContext ssc, JavaSparkContext sc) {
    System.out.println("--> Processing stream");
    JavaDStream<String> textStream = ssc.socketTextStream("localhost", 9999);
    System.out.println("--> Processing stream2");
    textStream = textStream.repartition(4);

    long[] totalCount = {0};

    JavaDStream<String> foo = textStream.filter(v1 -> v1.contains("foo"));


    foo.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
      long currentCount = rdd.count();
      System.out.println("Current count: " + currentCount);
      totalCount[0] += currentCount;
      System.out.println("Total count: " + totalCount[0]);
    });



    System.out.println("!!!Count: " + totalCount[0]);
  }

}

