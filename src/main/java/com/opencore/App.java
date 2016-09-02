package com.opencore;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

public class App {

  public static void main(String... args) {
    String logFile = "README.md";

    // Pure RDD based approach
    JavaSparkContext sc = new JavaSparkContext("local[1]", "testing");

    JavaRDD<String> readmeRdd = sc.textFile(logFile);
    JavaRDD<Integer> lengthRdd = readmeRdd.map(String::length);
    Integer combinedLength = lengthRdd.reduce((v1, v2) -> v1 + v2);
    System.out.println(combinedLength);

    // Spark SQL based approach
    SQLContext sqlContext = new SQLContext(sc);
    DataFrame df = sqlContext.read().text(logFile);
    df.registerTempTable("readme");
    DataFrame sql = sqlContext.sql("SELECT SUM(length(value)) FROM readme");
    Long combinedLengthLong = sql.first().getLong(0);
    System.out.println(combinedLengthLong);

    // This will fail because it's a long and not an int:
    //combinedLength = sql.first().getInt(0);

    // Dataframe approach
    JavaRDD<Integer> lengthRdd2 = df.javaRDD().map(value -> value.getString(0).length());
    combinedLength = lengthRdd2.reduce((v1, v2) -> v1 + v2);
    System.out.println(combinedLength);

    // Dataset approach
    Dataset<String> dataset = sqlContext.createDataset(readmeRdd.rdd(), Encoders.STRING());
    Dataset<Integer> lengthDataset = dataset.map((MapFunction<String, Integer>) String::length, Encoders.INT());
    combinedLength = lengthDataset.reduce((ReduceFunction<Integer>) (v1, v2) -> v1 + v2);
    System.out.println(combinedLength);
  }

}
