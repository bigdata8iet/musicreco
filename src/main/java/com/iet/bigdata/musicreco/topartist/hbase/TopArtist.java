package com.iet.bigdata.musicreco.topartist.hbase;

import org.apache.hadoop.hbase.util.Bytes;


public class TopArtist {

	
	private int artistid;
	private int count;

	static byte[] res1 = new byte[4];
	static byte[] res2 = new byte[4];
	static byte[] result = new byte[8];
	
	public TopArtist(){
		this.artistid = 0;
		this.count = 0;
	}
	
	public TopArtist(int id, int count){
		this.artistid = id;
		this.count = count;
	}

	public int getArtistid() {
		return artistid;
	}

	public void setArtistid(int artistid) {
		this.artistid = artistid;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "TopArtists [artistid=" + artistid + ", count=" + count + "]";
	}

	public static byte[] pack(int artistid, int count){
		
		res1 = Bytes.toBytes(artistid);
		res2 = Bytes.toBytes(count);
		
		for(int i=0;i<4;i++){
			result[i] = res2[i];
			result[4+i] = res1[i];
		}
		return result;
	}

public byte[] pack(){
		
		result[0] = (byte) (count >> 24);
		result[1] = (byte) (count >> 16);
		result[2] = (byte) (count >> 8);
		result[3] = (byte) (count);

		result[4] = (byte) (artistid >> 24);
		result[5] = (byte) (artistid >> 16);
		result[6] = (byte) (artistid >> 8);
		result[7] = (byte) (artistid);
		
		return result;
	}

	public static TopArtist unpack(byte[] key) {
		
		for(int i=0;i<4;i++){
			res1[i] = key[i];
			res2[i] = key[4+i];
		}
		return new TopArtist(Bytes.toInt(res2),Bytes.toInt(res1));
	}

}
