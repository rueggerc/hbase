package com.rueggerllc.hbase.tests;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class MapReduceTests {

	private static Logger logger = Logger.getLogger(MapReduceTests.class);
	
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
	
	
	
	
	
}
