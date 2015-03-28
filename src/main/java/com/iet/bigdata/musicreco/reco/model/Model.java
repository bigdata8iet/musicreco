package com.iet.bigdata.musicreco.reco.model;

import java.util.Arrays;

public class Model {

	int user_id;
	byte rating;
	int song_id;
	
	public int getSong_id() {
		return song_id;
	}

	public void setSong_id(int song_id) {
		this.song_id = song_id;
	}

	public byte getRating() {
		return rating;
	}

	public void setRating(byte rating) {
		this.rating = rating;
	}

	public int getUser_id() {
		return user_id;
	}

	public void setUser_id(int user_id) {
		this.user_id = user_id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + rating;
		result = prime * result + song_id;
		result = prime * result + user_id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SongRating other = (SongRating) obj;
		if (rating != other.rating)
			return false;
		if (song_id != other.song_id)
			return false;
		if (user_id != other.user_id)
			return false;
		return true;
	}

	
	public String toBinaryStr(byte[] bytes) {
		return Arrays.toString(bytes);
	}
}