package com.rueggerllc.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

/*
 *  Notes:
 *  
 *  Generic Parameters for class:
 *  Output key and value types.
 *  Key=byte array
 *  Value=integer
 *  
 *  Allows Hadoop to serialize/deserialize datatypes from underlying HDFS Store.
 *  ImmutableBytesWritable 		Datatype of our Output Key. Wrapper around a byte array.
 *  IntWritable                 Datatype of our Output Variable. Writeable wrapper around int
 *   
 */

public class MaritalStatusMapper extends TableMapper<ImmutableBytesWritable, IntWritable>  {

	private static final byte[] COLUMN_FAMILY = "personal".getBytes();
	private static final byte[] MARITAL_STATUS = "marital_status".getBytes();
	
	private final IntWritable ONE = new IntWritable(1);
	private ImmutableBytesWritable key = new ImmutableBytesWritable();
	
	//
	// Input Parameters are input (Key,Value) pair from HBase table
	// rowKey for record
	// value  represents 1 record
	public void map(ImmutableBytesWritable rowKey, Result value, Context context) throws IOException, InterruptedException {
		
		// Extract Marital Status Value from table and set that as key to our output mapping
		key.set(value.getValue(COLUMN_FAMILY, MARITAL_STATUS));
		
		// Set the value of our output mapping to 1
		context.write(key, ONE);
	}
	
}
