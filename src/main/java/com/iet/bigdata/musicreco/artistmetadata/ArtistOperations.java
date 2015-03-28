package com.iet.bigdata.musicreco.artistmetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;






public class ArtistOperations extends Thread{
	public static Configuration conf = null;
	private static HColumnDescriptor family = null;
	static {
		conf = HBaseConfiguration.create();
	}
	
	public static void useTable(String tableName, byte[] families, byte[] qualifier) throws Exception {

		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.isTableAvailable(tableName.getBytes())) {
			System.out.println(tableName + " Table already exists!");
			System.out.println("Using Existing Table" + tableName);
		} 
		else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName.getBytes());
			family = new HColumnDescriptor(families);
			tableDesc.addFamily(family);

			admin.createTable(tableDesc);
			System.out.println("Table Created " + tableName + " ok.");
		}
	}
	
	
	public static HTable getTable() throws Exception {
		String tableName = "ArtistTable";
		HTable table = new HTable(conf, tableName);
		return table;
	}
	
	
	public static void addRecord(HTable htable, byte[] families,
			byte[] qualifier, byte[] rowKey, byte[] value) throws Exception {
		
		int counter=0;
		Put put = new Put(rowKey);
		put.add(families, qualifier, value);
		htable.put(put);
		counter++;
		
		if (counter % 1000 == 0) {

			System.out.println(counter + " Records Inserted To Table "
					+ htable.getName());
			sleep((long) 500);
			System.gc();
			System.runFinalization();
		}
	
	}
	
}

