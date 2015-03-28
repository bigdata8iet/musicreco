package com.iet.bigdata.musicreco.reco.model;


public class UserRating extends Model{

	public UserRating() {
		this(0, (byte) 0, 0);
	}

	public UserRating(int user_id, byte rating, int song_id) {
		super();
		this.user_id = user_id;
		this.rating = rating;
		this.song_id = song_id;
	}

	public byte[] pack(byte[] result) {

		
		result[0] = (byte) (user_id >> 24);
		result[1] = (byte) (user_id >> 16);
		result[2] = (byte) (user_id >> 8);
		result[3] = (byte) (user_id);

		result[4] = rating;

		result[5] = (byte) (song_id >> 24);
		result[6] = (byte) (song_id >> 16);
		result[7] = (byte) (song_id >> 8);
		result[8] = (byte) (song_id);

		return result;
	}

	public static UserRating unpack(byte[] bytes) {

		int songId_ = ((bytes[5] & 0xff) << 24) 
				| ((bytes[6] & 0xff) << 16)
				| ((bytes[7] & 0xff) << 8) 
				| (bytes[8] & 0xff);

		byte rating_ = bytes[4];

		int userId_ = ((bytes[0] & 0xff) << 24) 
				| ((bytes[1] & 0xff) << 16)
				| ((bytes[2] & 0xff) << 8) 
				| (bytes[3] & 0xff);

		return new UserRating(userId_, rating_, songId_);
	}
	
	@Override
	public String toString() {
		return "userRating [user_id=" + user_id + ", rating=" + rating
				+ ", song_id=" + song_id + "]";
	}
}