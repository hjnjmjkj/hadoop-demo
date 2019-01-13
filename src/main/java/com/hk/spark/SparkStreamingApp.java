package com.hk.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingApp {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("n1", 9999);
		
		//切割压平
		JavaDStream<String> words = lines.flatMap(t -> Arrays.asList(t.split(" ")));
	    //和1组合
		JavaPairDStream<String, Integer>  jprdd = words.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
	    //分组聚合
		JavaPairDStream<String, Integer> wordCounts  = jprdd.reduceByKey((a, b) -> a + b);
	
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
	}
}
