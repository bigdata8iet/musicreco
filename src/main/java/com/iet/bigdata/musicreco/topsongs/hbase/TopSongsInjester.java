package com.iet.bigdata.musicreco.topsongs.hbase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.hbase.client.HTable;

public class TopSongsInjester {

	public static void main(String[] args) throws Exception {

		FileInputStream fs = new FileInputStream(
				"/mnt/data/workspace/music/test/TopSongsOutput/part-r-00000");
		InputStreamReader isr = new InputStreamReader(fs);
		BufferedReader br = new BufferedReader(isr);

		String line = null, str[] = new String[3];
		TopSongsHBaseOperations.useTable();
		HTable topRatedSongsTable = TopSongsHBaseOperations.getTable();
		TopSong tr = new TopSong();

		byte bytes[] = null;
		while ((line = br.readLine()) != null) {

			str = line.split("\t", 2);
			tr.setSongid(Integer.parseInt(str[0]));
			tr.setCount(Integer.parseInt(str[1]));
			bytes = tr.pack();
			TopSongsHBaseOperations.addRecord(topRatedSongsTable, bytes);

		}
		System.out.println("Insertion Successful");
	}

}
