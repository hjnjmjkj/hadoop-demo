package com.hk.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text mapOutputKey=new Text();
		private final static IntWritable mapOutputValue =new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String lineValue=value.toString();
			
			StringTokenizer stringTokenizer=new StringTokenizer(lineValue);
			
			while(stringTokenizer.hasMoreTokens()){
				String worldValue=stringTokenizer.nextToken();
				
				mapOutputKey.set(worldValue);
				
				context.write(mapOutputKey, mapOutputValue);
				
			}
		}
		
	}
	public static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable outputValue =new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values){
				sum +=value.get();
			}
			
			outputValue.set(sum);
			
			context.write(key, outputValue);
		}
		
	}
	
	public int run(String [] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration configuration=new Configuration();
		
		Job job=Job.getInstance(configuration, this.getClass().getSimpleName());
	
		job.setJarByClass(this.getClass());
		
		Path inPath =new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);
		
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path outPath =new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		
		boolean isSuccess= job.waitForCompletion(true);
		
		return isSuccess?0:1;
		
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		int status=new WordCount().run(args);
		
		System.exit(status);
	}
}
