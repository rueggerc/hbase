package com.rueggerllc.hbase.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class FilterSensors {

	private static Logger logger = Logger.getLogger(FilterSensors.class);
	
	private static final String HBASE_CONFIG_ZOOKEEPER_QUORUM = "captain,godzilla,darwin";
	private static final String HBASE_CONFIG_ZOOKEEPER_CLIENTPORT = "2181";
	
	// Column Families
	private static byte[] DHT22_CF = Bytes.toBytes("dht22");
	private static byte[] SENSEHATL_CF = Bytes.toBytes("sensehat");
	
	// DHT22
	private static byte[] SENSORID_COLUMN = Bytes.toBytes("sensor_id");
	private static byte[] TEMPERATURE_COLUMN = Bytes.toBytes("temperature");
	private static byte[] HUMIDITY_COLUMN = Bytes.toBytes("humidity");
	private static byte[] LOCATION_COLUMN = Bytes.toBytes("location");
	private static byte[] TIMESTAMP_COLUMN = Bytes.toBytes("timestamp");
	
	
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
	@Ignore
	public void testScan() throws Exception {
		
		logger.info("TEST: ScanWithFilter");
		Connection connection = this.getConnection();
		
		Table table = null;
		// Implements Iterable<Result> means we can use forEach Loop
		ResultScanner scanResult = null;
		try {
			table = connection.getTable(TableName.valueOf("sensors"));
			
			Scan scan = new Scan();
			scanResult = table.getScanner(scan);
			printResults(scanResult);
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
			if (scanResult != null) {
				scanResult.close();
			}
		}
		
	}
	
	@Test
	@Ignore
	public void testScanFilterOnRowKeys() throws Exception {
		
		logger.info("TEST: ScanWithFilter");
		Connection connection = this.getConnection();
		
		Table table = null;
		// Implements Iterable<Result> means we can use forEach Loop
		ResultScanner scanResult = null;
		try {
			table = connection.getTable(TableName.valueOf("sensors"));
			
			// Where RowKey == 4
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("4")));
			
			// Where RowKey < 2
			Filter filterLessThan = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("2")));
			
			Scan scan = new Scan();
			scan.setFilter(filter);
			
			scanResult = table.getScanner(scan);
			this.printResults(scanResult);
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
			if (scanResult != null) {
				scanResult.close();
			}
		}
		
	}
	
	@Test
	@Ignore
	public void testScanFilterOnSingleColumnValue() throws Exception {
		
		logger.info("TEST: ScanWithFilter");
		Connection connection = this.getConnection();
		
		Table table = null;
		// Implements Iterable<Result> means we can use forEach Loop
		ResultScanner scanResult = null;
		try {
			table = connection.getTable(TableName.valueOf("sensors"));
			
			// WHERE dht22:sensor_id == 'sensor1'
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("dht22"),
				Bytes.toBytes("sensor_id"),
				CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("sensor1")));
			
			// Exludes records where column is missing
			filter.setFilterIfMissing(true);
			
			Scan scan = new Scan();
			scan.setFilter(filter);
			
			scanResult = table.getScanner(scan);
			printResults(scanResult);
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
			if (scanResult != null) {
				scanResult.close();
			}
		}
		
	}
	
	@Test
	@Ignore
	public void testScanFilterOnSubtring() throws Exception {
		
		logger.info("TEST: ScanWithFilter");
		Connection connection = this.getConnection();
		
		Table table = null;
		// Implements Iterable<Result> means we can use forEach Loop
		ResultScanner scanResult = null;
		try {
			table = connection.getTable(TableName.valueOf("sensors"));
			
			// WHERE dht22:sensor_id like '%sensor1%'
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("dht22"),
				Bytes.toBytes("sensor_id"),
				CompareFilter.CompareOp.EQUAL,
				new SubstringComparator("Jones"));
			
			// Excludes records where column is missing
			filter.setFilterIfMissing(true);
			
			Scan scan = new Scan();
			scan.setFilter(filter);
			
			scanResult = table.getScanner(scan);
			printResults(scanResult);
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
			if (scanResult != null) {
				scanResult.close();
			}
		}
		
	}
	
	@Test
	// @Ignore
	public void testFilterOnMultipleColumnValue() throws Exception {
		
		logger.info("TEST: ScanWithFilter");
		Connection connection = this.getConnection();
		
		Table table = null;
		// Implements Iterable<Result> means we can use forEach Loop
		ResultScanner scanResult = null;
		try {
			table = connection.getTable(TableName.valueOf("sensors"));
			
			// WHERE dht22:sensor_id == 'sensor1'
			SingleColumnValueFilter idFilter = new SingleColumnValueFilter(
				Bytes.toBytes("dht22"),
				Bytes.toBytes("sensor_id"),
				CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("sensor1")));			
			idFilter.setFilterIfMissing(true);
			
			// WHERE dht22:temperature > 70
			SingleColumnValueFilter temperatureFilter = new SingleColumnValueFilter(
				Bytes.toBytes("dht22"),
				Bytes.toBytes("temperature"),
				CompareFilter.CompareOp.GREATER,
				new BinaryComparator(Bytes.toBytes("70")));			
			temperatureFilter.setFilterIfMissing(true);
			
			// Combine Filters
			// Select * from sensors
			// WHERE sensor_id = 'sensor1'
			// AND
			// temperature > 70
			List<Filter> filterList = new ArrayList<>();
			filterList.add(idFilter);
			filterList.add(temperatureFilter);
			FilterList filters = new FilterList(filterList);
			
			Scan scan = new Scan();
			scan.setFilter(filters);
			
			scanResult = table.getScanner(scan);
			printResults(scanResult);
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
			if (scanResult != null) {
				scanResult.close();
			}
		}
		
	}
	
	
	
	
	private Connection getConnection() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		return connection;
	}
	
	private void printResults(ResultScanner scanResult) {
		for (Result nextResult : scanResult) {
			
			logger.info("NEXT ROW KEY");
			
			// Value indexed by 4 Dimensions:
			// RowKey, ColumnFamily, Column, Timestamp
			// cloneX acts as extract method
			for (Cell cell : nextResult.listCells()) {
				String row = new String(CellUtil.cloneRow(cell));
				String family = new String(CellUtil.cloneFamily(cell));
				String column = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				logger.info(row + " " + family + " " + column + " "  + value);
			}
		}		
	}
	
	


	
}
