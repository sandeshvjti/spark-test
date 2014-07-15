package com.spark.app;


import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class WordCount {
  public static void main(String[] args) {
	  
	  if (args.length < 3) {
	      System.err.println("Usage: WordCount <file>");
	      System.exit(1);
	    }
    String logFile = args[1]; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("WordCount");
    conf.setMaster(args[0]);
    
    String[] jars = new String[] { args[2]};
    conf.setJars(jars);
    
    
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}