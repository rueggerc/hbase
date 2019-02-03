package com.rueggerllc.hbase.apps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import com.rueggerllc.hbase.mapreduce.MaritalStatusMapper;
import com.rueggerllc.hbase.mapreduce.MaritalStatusReducer;

public class MaritalStatusApp {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Job job = Job.getInstance(conf);
		job.setJarByClass(MaritalStatusApp.class);
		
		// Tables
		String sourceTable = "census3";
		String targetTable = "summary";
		
		// Setup Scan
		// default caching of HBase table rows=1. Inefficient for MapReduce
		// Read 500 rows at a time. Set to some high number.
		// cacheBlocks : whatever you are reading from HBase table, do not store in cache memory. Not being re-used.
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		// Tell Hadoop what Jars our job depends on.
		// Helper Utility. Pull all dependencies of job, make available to Hadoop.
		TableMapReduceUtil.addDependencyJars(job);
		
		// Mapper Job
		// sourceTable
		// scan: operation on source table to extract records
		// MaritalStatusMapper: mapper class
		// ImmutableBytesWritable: datatype of output key 
		// IntWritable : datatype for output value
		TableMapReduceUtil.initTableMapperJob(
				sourceTable,
				scan,
				MaritalStatusMapper.class,
				ImmutableBytesWritable.class,
				IntWritable.class,
				job);
		
		// Reducer
		TableMapReduceUtil.initTableReducerJob(
				targetTable,
				MaritalStatusReducer.class,
				job);
		
		
		// How many reduce processes to run.
		// Default is 1
		job.setNumReduceTasks(1);
		
		// Submit Job
		job.waitForCompletion(true);
		
		
	}
	
}
