package com.iet.bigdata.musicreco.topsongs.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class TopSong {

	private int songid;
	private int count;
	static byte[] res1 = new byte[4];
	static byte[] res2 = new byte[4];
	static byte[] result = new byte[8];
	
	public TopSong(){
		this.songid=0;
		this.count=0;
	}
	
	public TopSong(int songid,int count){
		this.songid=songid;
		this.count=count;
	}
	
	
	
	public int getSongid() {
		return songid;
	}
	
	public void setSongid(int songid) {
		this.songid = songid;
	}
	
	public int getCount() {
		return count;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
	@Override
	public String toString() {
		return "TopRatedSongs [songid=" + songid + ", count=" + count + "]";
	}
	
	public static byte[] pack(int songid, int count){
		
		res1 = Bytes.toBytes(songid);
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

		result[4] = (byte) (songid >> 24);
		result[5] = (byte) (songid >> 16);
		result[6] = (byte) (songid >> 8);
		result[7] = (byte) (songid);
		
		return result;
	}
	
	public static TopSong unpack(byte[] key) {
		
		for(int i=0;i<4;i++){
			res1[i] = key[i];
			res2[i] = key[4+i];
		}
		return new TopSong(Bytes.toInt(res2),Bytes.toInt(res1));
	}
	
}
