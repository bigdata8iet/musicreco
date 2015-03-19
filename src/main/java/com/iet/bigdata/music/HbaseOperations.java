package com.iet.bigdata.music;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperations extends Thread {
	private static Configuration conf = null;
	private static HColumnDescriptor family = null;
	static {
		conf = HBaseConfiguration.create();
	}

	public static void useTable(final Class<? extends Model> modelClass,
			byte[] families, byte[] qualifier) throws Exception {

		String tableName = modelClass.getSimpleName() + "Table";
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (admin.isTableAvailable(tableName.getBytes())) {
			System.out.println(tableName + " Table already exists!");
			System.out.println("Using Existing Table" + tableName);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					tableName.getBytes());

			family = new HColumnDescriptor(families);
			tableDesc.addFamily(family);

			admin.createTable(tableDesc);
			System.out.println("Table Created " + tableName + " ok.");
		}

	}

	public static HTable getTable(final Class<? extends Model> modelClass)
			throws Exception {

		String tableName = modelClass.getSimpleName() + "Table";
		HTable table = new HTable(conf, tableName);
		return table;
	}

	public static void addRecord(HTable htable, byte[] families,
			byte[] qualifier, byte[] rowKey) throws Exception {
		try {
			int counter = 0;

			Put put = new Put(rowKey);
			put.add(families, qualifier, rowKey);
			htable.put(put);
			counter++;

			if (counter % 1000 == 0) {

				System.out.println(counter + " Records Inserted To Table "
						+ htable.getName());
				sleep((long) 500);
				System.gc();
				System.runFinalization();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Set<SongRating> selectSongRating(SongRating start,
			SongRating end) throws Exception {
		byte[] res1 = new byte[9];
		byte[] res2 = new byte[9];

		Set<SongRating> ssr = new HashSet<SongRating>();
		Scan scan = new Scan(start.pack(res1), end.pack(res2));
		HTable Songtable = getTable(SongRating.class);
		ResultScanner scanner = Songtable.getScanner(scan);

		// Set<SongRating> mm = new HashSet<SongRating>();

		for (Result r : scanner) {
			ssr.add(SongRating.unpack(r.getRow()));

		}
		scanner.close();
		return ssr;
	}

	public static Set<Integer> selectUserRating(UserRating start, UserRating end)
			throws Exception {

		Set<Integer> songRecommend = new HashSet<Integer>();
		byte[] res1 = new byte[9];
		byte[] res2 = new byte[9];
		UserRating ur = new UserRating();

		Scan scan = new Scan(start.pack(res1), end.pack(res2));
		HTable Usertable = getTable(UserRating.class);
		ResultScanner scanner = Usertable.getScanner(scan);

		for (Result r : scanner) {
			ur = UserRating.unpack(r.getRow());

			songRecommend.add(ur.getSong_id());

		}
		scanner.close();

		return songRecommend;
	}

	public static Iterator<Model> getAllRecords(
			final Class<? extends Model> modelClass) throws IOException {

		Iterator<Model> dataIterator = new Iterator<Model>() {
			String tableName = modelClass.getSimpleName() + "Table";
			HTable table = new HTable(conf, tableName);
			ResultScanner scanner = table.getScanner(Bytes.toBytes("MyFamily"));
			Iterator<Result> resultIterator = scanner.iterator();

			public void remove() {
				resultIterator.remove();
			}

			public Model next() {
				Result result = resultIterator.next();
				// byte[] bytes = result.getRow(); // Assuming getRow returns
				// the key...
				byte[] bytes = result.getValue(Bytes.toBytes("MyFamily"),
						Bytes.toBytes("Qualifier"));
				Model rating = null;
				if (modelClass == UserRating.class) {
					rating = UserRating.unpack(bytes);
				} else if (modelClass == SongRating.class) {
					rating = SongRating.unpack(bytes);
				} else {
					throw new RuntimeException("Class not supported: "
							+ modelClass);
				}

				return rating;
			}

			public boolean hasNext() {
				return resultIterator.hasNext();
			}
		};

		return dataIterator;
	}
}