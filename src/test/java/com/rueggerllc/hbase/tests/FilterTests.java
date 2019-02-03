package com.rueggerllc.hbase.tests;

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
import org.apache.hadoop.hbase.filter.FilterList.Operator;
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

public class FilterTests {

	private static Logger logger = Logger.getLogger(FilterTests.class);
	
	private static final String HBASE_CONFIG_ZOOKEEPER_QUORUM = "captain,godzilla,darwin";
	private static final String HBASE_CONFIG_ZOOKEEPER_CLIENTPORT = "2181";
	
	// Column Families
	private static byte[] PERSONAL_CF = Bytes.toBytes("personal");
	private static byte[] PROFESSIONAL_CF = Bytes.toBytes("professional");
	
	// Personal
	private static byte[] NAME_COLUMN = Bytes.toBytes("name");
	private static byte[] GENDER_COLUMN = Bytes.toBytes("gender");
	private static byte[] MARITAL_STATUS_COLUMN = Bytes.toBytes("marital_status");
	
	// Professional
	private static byte[] EMPLOYED_COLUMN = Bytes.toBytes("employed");
	private static byte[] FIELD_COLUMN = Bytes.toBytes("field");
	
	
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
	
	// SETUP FOR THESE TESTS
	//	create 'census3', 'personal', 'professional'
	//	put 'census3', 1, 'personal:name', 'Mike Jones'
	//	put 'census3', 1, 'personal:marital_status', 'unmarried'
	//	put 'census3', 1, 'personal:gender', 'male'
	//	put 'census3', 1, 'professional:employed', 'yes'
	//	put 'census3', 1, 'professional:education_level', 'high school'
	//	put 'census3', 1, 'professional:field', 'construction'
	//
	//	put 'census3', 3, 'personal:name', 'Jill Tang'
	//	put 'census3', 3, 'personal:marital_status', 'married'
	//	put 'census3', 3, 'personal:spouse', 'Jim Tang'
	//	put 'census3', 3, 'professional:education_level', 'post-grad'
	//	put 'census3', 3, 'personal:gender', 'female'
	//
	//	put 'census3', 2, 'personal:name', 'Ben'
	//	put 'census3', 2, 'personal:marital_status', 'divorced'
	//	put 'census3', 2, 'personal:gender', 'male'
	//
	//	put 'census3', 4, 'personal:name', 'Maria'
	//	put 'census3', 4, 'personal:marital_status', 'divorced'
	//	put 'census3', 4, 'personal:gender', 'female'	
	
	
	@Test
	@Ignore
	public void testFilterOnRowKeys() throws Exception {
		
		logger.info("Run Filters Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = null;
		ResultScanner scanResult = null;
		try {
			
			table = connection.getTable(TableName.valueOf("census3"));
			// SELECT * from census3 where rowKey = 1
			// Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("4")));
			
			// SELECT * from census3 where rowKey < 2
			Filter filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("2")));
			
			Scan userScan = new Scan();
			userScan.setFilter(filter);
			scanResult = table.getScanner(userScan);
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
	public void testFilterOnColumnValues() throws Exception {
		
		logger.info("Run Filters Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = null;
		ResultScanner scanResult = null;
		try {
			
			table = connection.getTable(TableName.valueOf("census3"));
			
			// SELECT * from census3 where gender = 'male'
			logger.info("============ Column Filter Test 2 =====");
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					Bytes.toBytes("personal"),
					Bytes.toBytes("gender"),
					CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes("male")));
			
			// Exclude all records with gender field missing.
			filter.setFilterIfMissing(true);
			Scan userScan = new Scan();
			userScan.setFilter(filter);
			scanResult = table.getScanner(userScan);
			printResults(scanResult);
			
			// SELECT * from census3 where name like "%Jones%"
			// Matches "Mike Jones"
			logger.info("============ Column Filter Test 2 =====");
			filter = new SingleColumnValueFilter(
					Bytes.toBytes("personal"),
					Bytes.toBytes("name"),
					CompareFilter.CompareOp.EQUAL,
					new SubstringComparator("Jones"));
			userScan.setFilter(filter);
			scanResult = table.getScanner(userScan);
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
	public void testFilterLogicalAND() throws Exception {
		
		logger.info("Run Filters AND Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = null;
		ResultScanner scanResult = null;
		try {
			
			table = connection.getTable(TableName.valueOf("census3"));
			
			// SELECT *
			// FROM census3
			// WHERE marital_status = 'divorced' 
			SingleColumnValueFilter maritalStatusFilter = new SingleColumnValueFilter(
					Bytes.toBytes("personal"),
					Bytes.toBytes("marital_status"),
					CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes("divorced")));
			maritalStatusFilter.setFilterIfMissing(true);
			
			// SELECT *
			// FROM census3
			// WHERE gender = 'male' 
			SingleColumnValueFilter genderFilter = new SingleColumnValueFilter(
					Bytes.toBytes("personal"),
					Bytes.toBytes("gender"),
					CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes("male")));
			genderFilter.setFilterIfMissing(true);
			
			
			// SELECT * 
			// FROM census3
			// WHERE marital_status = 'divorced' AND gender = 'male'
			List<Filter> filterList = new ArrayList<Filter>();
			filterList.add(maritalStatusFilter);
			filterList.add(genderFilter);
			// FilterList filters =  new FilterList(filterList);
			FilterList filters =  new FilterList(Operator.MUST_PASS_ALL,filterList);
			
			// Execute Query
			// Default is AND
			logger.info("Logical AND");
			Scan userScan = new Scan();
			userScan.setFilter(filters);
			scanResult = table.getScanner(userScan);
			printResults(scanResult);
			
			
			// SELECT * 
			// FROM census3
			// WHERE marital_status = 'divorced' OR gender = 'male'			
			filters =  new FilterList(Operator.MUST_PASS_ONE,filterList);
			
			// Execute Query
			// Default is AND
			logger.info("Logical OR");
			userScan.setFilter(filters);
			scanResult = table.getScanner(userScan);
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
	

	
	private static void printResults(ResultScanner scanResults) {
		for (Result result : scanResults) {
			for (Cell cell : result.listCells()) {
				String row = new String(CellUtil.cloneRow(cell));
				String family = new String(CellUtil.cloneFamily(cell));
				String column = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				logger.info(row + " " + family + " " + column + " "  + value);				
			}
		}
	}
	
	
	
	
}
