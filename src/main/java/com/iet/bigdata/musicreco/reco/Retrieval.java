package com.iet.bigdata.musicreco.reco;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

import com.iet.bigdata.musicreco.reco.model.Model;
import com.iet.bigdata.musicreco.reco.model.SongRating;
import com.iet.bigdata.musicreco.reco.model.UserRating;

public class Retrieval {

	public static void main(String[] args) throws Exception {
		
		HbaseOperations hb=new HbaseOperations();
		UserRating ur=null;
		SongRating sr=null;
		
		
			
		byte[] family = Bytes.toBytes("MyFamily");
		byte[] qualifier = Bytes.toBytes("Qualifier");
		byte[] result1 = null;
		
		
		
		hb.useTable(UserRating.class, family, qualifier);
		int count=0;
		Iterator <Model> dataIterator=hb.getAllRecords(UserRating.class);
		
		while(dataIterator.hasNext()){
			System.out.println((UserRating)dataIterator.next());
			count++;
		}
		System.out.println(count);

		hb.useTable(SongRating.class, family, qualifier);
		count=0;
		dataIterator=hb.getAllRecords(SongRating.class);
		
		while(dataIterator.hasNext()){
			System.out.println((SongRating)dataIterator.next());
			count++;
		}
		System.out.println(count);	
	}

}