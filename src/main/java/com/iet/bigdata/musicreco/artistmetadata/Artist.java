package com.iet.bigdata.musicreco.artistmetadata;


public class Artist{
	
	int artist_id=0;
	String artist_name=null;
	static byte[] res1=new byte[30];
	
	public Artist(){
		this(0,"");
	}
	
	public Artist(int artist_id, String artist_name) {
		super();
		this.artist_id = artist_id;
		this.artist_name = artist_name;
	}
	
	public int getArtist_id() {
		return artist_id;
	}

	public void setArtist_id(int artist_id) {
		this.artist_id = artist_id;
	}

	public String getArtist_name() {
		return artist_name;
	}

	public void setArtist_name(String artist_name) {
		this.artist_name = artist_name;
	}
   
	
	
	@Override
	public String toString() {
		return "artistdata [artist_id=" + artist_id + ", artist_name="
				+ artist_name + "]";
	}

	
}
	
	
	/*public byte[] pack(byte[] result){
		
		res1=Bytes.toBytes(artist_name);
		result[0] = (byte) (artist_id >> 24);
		result[1] = (byte) (artist_id >> 16);
		result[2] = (byte) (artist_id >> 8);
		result[3] = (byte) (artist_id);
		
		for(int i=0;i<30;i++){
			result[4+i] = res1[i]; 
		}
		
		return result;
	}
	
	public static Artistdata unpack(byte[] bytes) {

		int artistid_ = ((bytes[0] & 0xff) << 24) 
				| ((bytes[1] & 0xff) << 16)
				| ((bytes[2] & 0xff) << 8) 
				| (bytes[3] & 0xff);
		
		for(int i=0;i<30;i++)
		{
			res1[i]=bytes[4+i];
		}

		return new Artistdata(artistid_,Bytes.toString(res1));
	}
*/

