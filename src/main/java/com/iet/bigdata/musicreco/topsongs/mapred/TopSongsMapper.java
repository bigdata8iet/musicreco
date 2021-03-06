package com.iet.bigdata.musicreco.topsongs.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopSongsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	IntWritable word = new IntWritable();
	String str = null;
	String arr[] = new String[3];
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		str = value.toString();
		arr = str.split("\t");
		
		if(Integer.parseInt(arr[2])==5){
			word.set(Integer.parseInt(arr[1]));
			context.write(word, one);
		}
		
		

	}
}
