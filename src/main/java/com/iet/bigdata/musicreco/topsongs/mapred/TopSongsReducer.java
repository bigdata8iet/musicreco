package com.iet.bigdata.musicreco.topsongs.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.iet.bigdata.musicreco.topsongs.hbase.TopSong;
import com.iet.bigdata.musicreco.topsongs.hbase.TopSongsHBaseOperations;



public class TopSongsReducer extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private IntWritable result = new IntWritable();
	TopSong tr = new TopSong();
	HTable topRatedSongsTable = null;
	
	public TopSongsReducer() throws Exception {
		
		topRatedSongsTable = TopSongsHBaseOperations.getTable();
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
		
		tr.setSongid(value);
		tr.setCount(sum);
		//System.out.println("songId: " +key+"\t count:" + sum);
		
		try {
			TopSongsHBaseOperations.addRecord(topRatedSongsTable, tr.pack());
		} catch (Exception e) {
			 e.printStackTrace();
		}
		
	}

}
