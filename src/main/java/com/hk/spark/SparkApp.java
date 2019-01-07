package com.hk.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public final class SparkApp {

  public static void main(String[] args) throws Exception {
	String input="hdfs://n1:8020/home/wordcount/input";
	String output="hdfs://n1:8020/home/wordcount/output2";

    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    //读取数据
    JavaRDD<String> lines = ctx.textFile(input, 1);
    //切割压平
    JavaRDD<String> jrdd2 = lines.flatMap(t -> Arrays.asList(t.split(" ")));
    //和1组合
    JavaPairRDD<String, Integer> jprdd = jrdd2.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
    //分组聚合
    JavaPairRDD<String, Integer> res = jprdd.reduceByKey((a, b) -> a + b);
    
    res.saveAsTextFile(output);
    
    ctx.stop();
  }
}

