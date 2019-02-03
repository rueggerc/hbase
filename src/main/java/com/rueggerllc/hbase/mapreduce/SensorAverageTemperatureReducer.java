package com.rueggerllc.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
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

public class SensorAverageTemperatureReducer extends TableReducer<ImmutableBytesWritable, FloatWritable, ImmutableBytesWritable>  {

	// Key,Value
	private static final byte[] COLUMN_FAMILY = "sensor_id".getBytes();
	private static final byte[] AVERAGE_TEMP = "avg_temperature".getBytes();
	
	public void reduce(ImmutableBytesWritable mappedKey, Iterable<FloatWritable> mappedValuesForKey, Context context) throws IOException, InterruptedException {
		
		Float sum = 0f;
		int count = 0;
		for (FloatWritable nextValue : mappedValuesForKey) {
			sum += nextValue.get();
		}
		Float average = 0f;
		if (count > 0) {
			average = sum / count;
		}
		
		
		// Use Put mutation to write key,value to result table
		Put put = new Put(mappedKey.get());
		put.addColumn(COLUMN_FAMILY, AVERAGE_TEMP, Bytes.toBytes(average.toString()));
		context.write(mappedKey, put);
	}
	
}
