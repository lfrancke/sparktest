package com.opencore;

import java.util.Date;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * This simple program demonstrates that jobs are scheduled one after the other.
 *
 * Job 1 will only start when job 0 has finished. If you need multiple jobs to run in parallel you need to submit
 * them from different threads.
 *
 * @see <a href="https://spark.apache.org/docs/latest/job-scheduling.html">Job Scheduling</a>
 */
public class JavaApp2 {

  public static void main(String... args) {
    String logFile = "README.md";

    JavaSparkContext sc = new JavaSparkContext("local[*]", "testing");

    sc.emptyRDD().repartition(2).foreachPartition(it -> {
      System.out.println("Job 0 - pre sleep (Thread: " + Thread.currentThread().getName() + ") " + new Date());
      Thread.sleep(2000);
      System.out.println("Job 0 - post sleep (Thread: " + Thread.currentThread().getName() + ") " + new Date());
    });

    sc.emptyRDD().repartition(1).foreachPartition(it -> {
      System.out.println("Job 1");
    });

  }

}
