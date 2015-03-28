package com.iet.bigdata.musicreco.topartist.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iet.bigdata.musicreco.topartist.hbase.TopArtistHBaseOperations;


public class TopArtistMain {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopRatedMapredJob");

		job.setJarByClass(TopArtistMain.class);
		job.setMapperClass(TopArtistMapper.class);
		job.setCombinerClass(TopArtistReducer.class);
		job.setReducerClass(TopArtistReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class); //integer

		FileInputFormat.addInputPath(job, new Path(
				"/mnt/data/workspace/artist/test/testartist.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/mnt/data/workspace/artist/test/TopArtistsOutput"));
		
		TopArtistHBaseOperations.useTable();

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
