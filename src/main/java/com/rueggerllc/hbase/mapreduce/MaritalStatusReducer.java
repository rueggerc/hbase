package com.rueggerllc.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

/*
 *  Notes:
 *  Write records to an HBbase table
 *  
 *  Generic Parameters for TableReducer:
 *  ImmutableBytesWritable		Output key from Map Phase
 *  IntWritable					Output value from Map Phase
 *  
 *  Third Parameter: 			Datatype of output key from reducer
 *  The output value type is the mutation to apply to the result table
 *  Mutation is an operation performed on an HBase table
 */

public class MaritalStatusReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>  {

	// Key,Value
	private static final byte[] COLUMN_FAMILY = "marital_status".getBytes();
	private static final byte[] COUNT = "count".getBytes();
	
	public void reduce(ImmutableBytesWritable mappedKey, Iterable<IntWritable> mappedValuesForKey, Context context) throws IOException, InterruptedException {
		
		Integer sum = 0;
		for (IntWritable nextValue : mappedValuesForKey) {
			sum += nextValue.get();
		}
		
		// Use Put mutation to write key,value to result table
		Put put = new Put(mappedKey.get());
		put.addColumn(COLUMN_FAMILY, COUNT, Bytes.toBytes(sum.toString()));
		context.write(mappedKey, put);
	}
	
}
