package com.hk.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseOperation {
	public static HTable getHTableByTableName(String tableName) throws Exception{
		Configuration configuation = HBaseConfiguration.create();
		
		HTable table = new HTable(configuation,tableName);
		
		return table;
	}
	
	public static void main(String[] args) throws Exception {
		String tableName = "user";
		
		HTable table = getHTableByTableName(tableName);
		
		Get get=new Get(Bytes.toBytes("10002"));
		
		Result result = table.get(get);
		
		for(Cell cell : result.rawCells()){
			System.out.println(
				Bytes.toString(CellUtil.cloneFamily(cell))+":"
				+Bytes.toString(CellUtil.cloneQualifier(cell))+"->"
				+Bytes.toString(CellUtil.cloneValue(cell))
			);
			
			
		}
	}
}
