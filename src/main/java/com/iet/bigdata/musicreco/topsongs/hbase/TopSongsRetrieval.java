package com.iet.bigdata.musicreco.topsongs.hbase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TopSongsRetrieval {

	public static void main(String[] args) throws Exception {
		
		TopSongsHBaseOperations.useTable();
		
		Map <Integer,Integer> map = TopSongsHBaseOperations.Topratedsongs();
		System.out.println(map.size());
		
		Set<Entry<Integer, Integer>> set = map.entrySet();
        List<Entry<Integer, Integer>> list = new ArrayList<Entry<Integer, Integer>>(set);
        Collections.sort( list, new Comparator<Map.Entry<Integer, Integer>>()
        {
            public int compare( Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
        System.out.println("Final set, after Sorting: ");
        
      //  list = list.subList(0, 10);
        for(Map.Entry<Integer, Integer> entry:list){
            System.out.println(entry.getKey()+": No. of Occurrences= "+entry.getValue());
        }

	}

}
