package com.iet.bigdata.musicreco.topsongs.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iet.bigdata.musicreco.topsongs.hbase.TopSongsHBaseOperations;


public class TopSongsMain {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopRatedMapredJob");

		job.setJarByClass(TopSongsMain.class);
		job.setMapperClass(TopSongsMapper.class);
		job.setCombinerClass(TopSongsReducer.class);
		job.setReducerClass(TopSongsReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class); //integer

		FileInputFormat.addInputPath(job, new Path(
				"/mnt/data/workspace/music/test/test_6.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/mnt/data/workspace/music/test/TopSongsOutput"));
		
		TopSongsHBaseOperations.useTable();

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
