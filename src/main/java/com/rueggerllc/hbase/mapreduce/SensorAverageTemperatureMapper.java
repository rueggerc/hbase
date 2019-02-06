package com.rueggerllc.hbase.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
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

public class SensorAverageTemperatureMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable>  {

	private static final byte[] COLUMN_FAMILY = "dht22".getBytes();
	private static final byte[] SENSOR_ID = "sensor_id".getBytes();
	private static final byte[] TEMPERATURE = "temperature".getBytes();
	
	private final IntWritable ONE = new IntWritable(1);
	private ImmutableBytesWritable key = new ImmutableBytesWritable();
	
	//
	// Input Parameters are input (Key,Value) pair from HBase table
	// rowKey for record
	// value  represents 1 record
	public void map(ImmutableBytesWritable rowKey, Result record, Context context) throws IOException, InterruptedException {
		
		// Extract Key Value from table and set that as key to our output mapping
		key.set(record.getValue(COLUMN_FAMILY, SENSOR_ID));
		
		// Set the value of our output mapping to the temperature
		byte[] temperatureBytes = record.getValue(COLUMN_FAMILY, TEMPERATURE);
		float temperature = ByteBuffer.wrap(temperatureBytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
		System.out.println("GOT TEMP=" + temperature);
		DoubleWritable temperatureValue = new DoubleWritable(temperature);
		context.write(key, temperatureValue);
	}
	
}
