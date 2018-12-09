package com.hk.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

public class HbaseOperation {
	public static void getHTableByTableName(String tableName)
			throws Exception {
		Configuration configuation = HBaseConfiguration.create();

		HTable table = new HTable(configuation, tableName);
		Get get = new Get(Bytes.toBytes("10003"));

		get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

		Result result = table.get(get);

		for (Cell cell : result.rawCells()) {
			System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
					+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
					+ Bytes.toString(CellUtil.cloneValue(cell)));

		}
		table.close();
	}

	public static void putHTableByTableName() throws Exception {
		String tableName = "user";

		Configuration configuation = HBaseConfiguration.create();

		HTable table = new HTable(configuation, tableName);
		
		Put put = new Put(Bytes.toBytes("10004"));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhaoliu"));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(25));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("address"),Bytes.toBytes("shanghai"));
		table.put(put);
		
		table.close();
		

	}
	public static void delHTableByTableName() throws Exception {
		String tableName = "user";
		
		Configuration configuation = HBaseConfiguration.create();
		
		HTable table = new HTable(configuation, tableName);
		
		Delete delete = new Delete(Bytes.toBytes("10004"));
		
		//delete.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("address"));
		delete.deleteFamily(Bytes.toBytes("info"));
		
		table.delete(delete);
		
		table.close();
		
	}
	public static void main(String[] args) throws Exception {
		String tableName = "user";
		HTable table =null;
		ResultScanner scanner=null;
		try {
			Configuration configuation = HBaseConfiguration.create();
			
			table = new HTable(configuation, tableName);
			
			Scan scan =new Scan();
			
			scan.setStartRow(Bytes.toBytes("10002"));
			scan.setStopRow(Bytes.toBytes("10006"));
			
			//scan.setFilter(filter);
			
			scanner= table.getScanner(scan);
			
			for (Result result : scanner) {
				System.out.println("------------------------");
				System.out.println(Bytes.toString(result.getRow()));
				for (Cell cell : result.rawCells()) {
					System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
							+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
							+ Bytes.toString(CellUtil.cloneValue(cell)));

				}
			}
			
			
			
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			IOUtils.closeStream(scanner);
			IOUtils.closeStream(table);
		}
	}
}
