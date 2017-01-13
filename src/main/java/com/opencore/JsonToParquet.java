package com.opencore;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Simple Java Spark app that takes a JSON file and converts it to Parquet.
 */
public class JsonToParquet {

  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext("local[1]", "testing");
    SQLContext sqlContext = new SQLContext(sc);
    DataFrame df = sqlContext.read().json("generated.json");
    df.write().parquet("generated.parquet");
  }

}
