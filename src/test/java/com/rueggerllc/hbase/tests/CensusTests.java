package com.rueggerllc.hbase.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class CensusTests {

	private static Logger logger = Logger.getLogger(CensusTests.class);
	
	private static final String HBASE_CONFIG_ZOOKEEPER_QUORUM = "captain,godzilla,darwin";
	private static final String HBASE_CONFIG_ZOOKEEPER_CLIENTPORT = "2181";
	
	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	@Ignore
	public void dummyTest() {
		logger.info("Dummy Test");
	}
	
	@Test
	// @Ignore
	public void createTable() throws Exception {
		
		logger.info("Create Table Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		logger.info("========== GOT CONNECTION ============");
		try {
			Admin admin = connection.getAdmin();
			HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("census2"));
			
			// Column Families
			tableName.addFamily(new HColumnDescriptor("personal"));
			tableName.addFamily(new HColumnDescriptor("professional"));
			
			// Create Table
			if (!admin.tableExists(tableName.getTableName())) {
				logger.info("Creating the Census Table BEGIN");
				admin.createTable(tableName);
				logger.info("Creating the Census Table DONE");
			} else {
				logger.info("Census Table already exists");
			}
		
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
		}
		
	}

	
}
