package com.iet.bigdata.music;
import java.io.*;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;


public class IODemoMain {


	public static void main(String args[])throws Exception {
		
		FileInputStream fs=new FileInputStream("/mnt/data/workspace/music/test/test_6.txt");
		InputStreamReader isr=new InputStreamReader(fs);
		BufferedReader br=new BufferedReader(isr);
		
		UserRating ur=new UserRating();
		SongRating sr=new SongRating();
		
		byte[] family = Bytes.toBytes("MyFamily");
		byte[] qualifier = Bytes.toBytes("Qualifier");
		
		HbaseOperations hb=new HbaseOperations();
		hb.useTable(UserRating.class,family,qualifier);
		hb.useTable(SongRating.class,family,qualifier);
		
		HTable userTable = hb.getTable(UserRating.class);
		HTable songTable = hb.getTable(SongRating.class);
		

		
		String line=null, str[]=new String[3];
		byte bytes[]=new byte[9]; 
		while((line=br.readLine())!=null){
			
			str=line.split("\t", 3);
			
			ur.setUser_id(Integer.parseInt(str[0]));
			ur.setSong_id(Integer.parseInt(str[1]));
			ur.setRating(Byte.parseByte(str[2]));
			bytes=ur.pack(bytes);
			hb.addRecord(userTable, family, qualifier, bytes);
		
			sr.setUser_id(Integer.parseInt(str[0]));
			sr.setSong_id(Integer.parseInt(str[1]));
			sr.setRating(Byte.parseByte(str[2]));
			bytes=sr.pack(bytes);
			hb.addRecord(songTable, family, qualifier, bytes);
		}
	
	}

}
