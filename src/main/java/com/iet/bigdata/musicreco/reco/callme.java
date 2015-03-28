package com.iet.bigdata.musicreco.reco;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.iet.bigdata.musicreco.reco.model.SongRating;
import com.iet.bigdata.musicreco.reco.model.UserRating;

public class callme {

	public static void main(String[] args) throws Exception {
		try {
			HbaseOperations hb = new HbaseOperations();

			SongRating sr1 = new SongRating(46977, (byte) 4, 0);
			SongRating sr2 = new SongRating(46977, (byte) 5, 0);

			UserRating ur1 = new UserRating(0, (byte) 5, 0);
			UserRating ur2 = new UserRating(0, (byte) 6, 0);

			int i = 0;
			Map<Integer, Integer> songreco = new HashMap<Integer, Integer>();

			Set<SongRating> value = hb.selectSongRating(sr1, sr2);
			Iterator<SongRating> itr = value.iterator();

			while (itr.hasNext()) {
				i = itr.next().getUser_id();
				ur1.setUser_id(i);
				ur2.setUser_id(i);
				// System.out.print("\nUser:" + ur1.getUser_id());

				Set<Integer> check = hb.selectUserRating(ur1, ur2);

				for (Integer songId : check) {
					Integer count = songreco.get(songId);
					if (count == null || count == 0) {
						songreco.put(songId, 1);
					} else {
						songreco.put(songId, count + 1);
					}
				}
			}
			
			 System.out.println("Final set, before Sorting: " + songreco);
			
			// Sort songreco on values in descending order
			 Set<Entry<Integer, Integer>> set = songreco.entrySet();
	        List<Entry<Integer, Integer>> list = new ArrayList<Entry<Integer, Integer>>(set);
	        Collections.sort( list, new Comparator<Map.Entry<Integer, Integer>>()
	        {
	            public int compare( Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2 )
	            {
	                return (o2.getValue()).compareTo( o1.getValue() );
	            }
	        } );
	        System.out.println("Final set, after Sorting: ");
	        for(Map.Entry<Integer, Integer> entry:list){
	            System.out.println(entry.getKey()+" = "+entry.getValue());
	        }

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
