package com.iet.bigdata.musicreco.artistmetadata;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class ArtistMetadataInjester {
	public static void main(String[] args) throws Exception {
		
		FileInputStream fs = new FileInputStream("/mnt/data/workspace/artist/test/testname.txt");
		InputStreamReader isr = new InputStreamReader(fs);
		BufferedReader br = new BufferedReader(isr);
		
		Artist ad = new Artist();
		
		byte[] family = Bytes.toBytes("MyFamily");
		byte[] qualifier = Bytes.toBytes("Qualifier");
		
		ArtistOperations ao = new ArtistOperations();
		ao.useTable("ArtistTable", family, qualifier);
		
		HTable artistTable = ao.getTable();
		

		String line=null;
		int counter=0;
		byte bytes[]=new byte[4]; 
		byte value[] = null;
		while((line=br.readLine())!=null){
			counter++;
			bytes = Bytes.toBytes(Integer.parseInt(line.substring(0, (line.indexOf("\t")))));
			value = Bytes.toBytes(line.substring((line.indexOf("\t")+1), (line.length())));	
			
			System.out.println("userid" +line.substring(0, (line.indexOf("\t"))));
			System.out.println("artistname" +line.substring((line.indexOf("\t")+1), (line.length())));
			ao.addRecord(artistTable, family, qualifier, bytes, value);
		
		}
		System.out.println("counter value" +counter);
		
	}

}
