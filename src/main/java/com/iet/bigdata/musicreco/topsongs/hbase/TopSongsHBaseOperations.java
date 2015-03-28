package com.iet.bigdata.musicreco.topsongs.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TopSongsHBaseOperations extends Thread {
	private static Configuration conf = null;
	private static HColumnDescriptor family = null;
	final static byte[] families = Bytes.toBytes("MyFamily");
	final static byte[] qualifier=Bytes.toBytes("Qualifier");
	final static String tableName = "TopRatedSongsTable";
	static {
		conf = HBaseConfiguration.create();
	}

	public static void useTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.isTableAvailable(tableName.getBytes())) {
			System.out.println(tableName + " Table already exists!");
			System.out.println("Using Existing Table" + tableName);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName.getBytes());

			family = new HColumnDescriptor(families);
			tableDesc.addFamily(family);
			admin.createTable(tableDesc);
			System.out.println("Table Created " + tableName + " ok.");
		}

	}

	public static HTable getTable() throws Exception {

		HTable table = new HTable(conf, tableName);
		return table;
	}

	public static void addRecord(HTable htable,
			byte[] rowKey) throws Exception {
		try {
			Put put = new Put(rowKey);
			put.add(families, qualifier, rowKey);
			htable.put(put);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

/*	public static Map<Integer,Integer> Topratedsongs()
			throws Exception {
		
			Map<Integer,Integer> map = new HashMap<Integer,Integer>();
			TopRatedSongs tr = new TopRatedSongs();
			
			
			HTable table = getTable();
			ResultScanner scanner = table.getScanner(Bytes.toBytes("MyFamily"));
			Iterator<Result> resultIterator = scanner.iterator();
			Result r = null;
			while(resultIterator.hasNext())
			{
				r = resultIterator.next();
				
				tr= TopRatedSongs.unpack(r.getRow());
				
				if(tr.getCount()>100)
				map.put(tr.getSongid(), tr.getCount());
			}
			scanner.close();
		return map;
		} */
	
	public static Map<Integer, Integer> Topratedsongs() throws Exception {

		Map<Integer, Integer> map = new HashMap<Integer, Integer>();
		TopSong tr = new TopSong();

		Scan scan = new Scan();

		scan.setReversed(true);
		HTable table = getTable();
		ResultScanner scanner = table.getScanner(scan);
		Integer count = 0;
		int number = 0;
		for (Result r : scanner) {
			tr = TopSong.unpack(r.getRow());

			map.put(tr.getSongid(), tr.getCount());
			if(number >= 10){
				break;
			} else {
				number++;
			}
		}
		
		scanner.close();
		return map;
	}

	
}