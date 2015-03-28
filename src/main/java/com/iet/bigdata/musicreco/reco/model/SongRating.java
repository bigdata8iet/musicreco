package com.iet.bigdata.musicreco.reco.model;


public class SongRating extends Model{

	public SongRating() {
		this(0, (byte) 0, 0);
	}

	public SongRating(int song_id, byte rating, int user_id) {
		super();
		this.song_id = song_id;
		this.rating = rating;
		this.user_id = user_id;
	}

	public byte[] pack(byte[] result) {

		
		result[0] = (byte) (song_id >> 24);
		result[1] = (byte) (song_id >> 16);
		result[2] = (byte) (song_id >> 8);
		result[3] = (byte) (song_id);

		result[4] = rating;

		result[5] = (byte) (user_id >> 24);
		result[6] = (byte) (user_id >> 16);
		result[7] = (byte) (user_id >> 8);
		result[8] = (byte) (user_id);

		return result;
	}

	public static SongRating unpack(byte[] bytes) {

		int userId_ = ((bytes[5] & 0xff) << 24) 
				| ((bytes[6] & 0xff) << 16)
				| ((bytes[7] & 0xff) << 8) 
				| (bytes[8] & 0xff);

		byte rating_ = bytes[4];

		int songId_ = ((bytes[0] & 0xff) << 24) 
				| ((bytes[1] & 0xff) << 16)
				| ((bytes[2] & 0xff) << 8) 
				| (bytes[3] & 0xff);

		return new SongRating(songId_, rating_, userId_);
	}
	
	@Override
	public String toString() {
		return "SongRating [song_id=" + song_id + ", rating=" + rating
				+ ", user_id=" + user_id + "]";
	}
}