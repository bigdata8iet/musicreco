package com.iet.bigdata.musicreco.topartist.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.iet.bigdata.musicreco.topartist.hbase.TopArtist;
import com.iet.bigdata.musicreco.topartist.hbase.TopArtistHBaseOperations;


public class TopArtistReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private IntWritable result = new IntWritable();
	TopArtist tr = new TopArtist();
	HTable topRatedArtistsTable = null;
	
	public TopArtistReducer() throws Exception {
		
		topRatedArtistsTable = TopArtistHBaseOperations.getTable();
	}
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		int value = key.get(); 
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		result.set(sum);
		context.write(key, result);
		//System.out.println("songId: " +key+"\t count:" + sum);
		
		tr.setArtistid(value);
		tr.setCount(sum);
		
		try {
			TopArtistHBaseOperations.addRecord(topRatedArtistsTable,tr.pack());
		} catch (Exception e) {
			 e.printStackTrace();
		}
		
	}

}
