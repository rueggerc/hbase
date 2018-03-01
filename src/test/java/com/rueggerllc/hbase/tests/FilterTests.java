package com.rueggerllc.hbase.tests;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
	
	@Test
	@Ignore
	public void testPutData() throws Exception {
		
		logger.info("Insert Data Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf("census2"));

			// Put1
			Put put1 = new Put(Bytes.toBytes("1"));
			put1.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Mike Jones"));
			put1.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("male"));
			put1.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, Bytes.toBytes("unmarried"));
			put1.addColumn(PROFESSIONAL_CF, EMPLOYED_COLUMN, Bytes.toBytes("yes"));
			put1.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("construction"));
			table.put(put1);

			// Put2
			Put put2 = new Put(Bytes.toBytes("2"));
			put2.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Hillary Clinton"));
			put2.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("female"));
			put2.addColumn(PERSONAL_CF, MARITAL_STATUS_COLUMN, Bytes.toBytes("married"));
			put2.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("politics"));
			
			// Put3
			Put put3 = new Put(Bytes.toBytes("3"));
			put3.addColumn(PERSONAL_CF, NAME_COLUMN, Bytes.toBytes("Donald Trump"));
			put3.addColumn(PERSONAL_CF, GENDER_COLUMN, Bytes.toBytes("male"));
			put3.addColumn(PROFESSIONAL_CF, FIELD_COLUMN, Bytes.toBytes("politics, reality show"));
			
			// Put List
			List<Put> putList = new ArrayList<Put>();
			putList.add(put2);
			putList.add(put3);
			table.put(putList);
			
			
			logger.info("Data Inserted");

		
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
		}
		
	}

	@Test
	@Ignore
	public void testGetDataByRow() throws Exception {
		
		logger.info("Get Data Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf("census2"));

			// Get One Row
			Get get = new Get(Bytes.toBytes("1"));
			get.addColumn(PERSONAL_CF, NAME_COLUMN);
			get.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);
			
			Result result = table.get(get);
			byte[] nameValue = result.getValue(PERSONAL_CF, NAME_COLUMN);
			byte[] fieldValue = result.getValue(PROFESSIONAL_CF, FIELD_COLUMN);
			
			logger.info("Name= " + Bytes.toString(nameValue));
			logger.info("Field= " + Bytes.toString(fieldValue));
			
			// Get Multiple Rows
			List<Get> getList = new ArrayList<Get>();
			Get get2 = new Get(Bytes.toBytes("2"));
			get2.addColumn(PERSONAL_CF, NAME_COLUMN);
			
			Get get3 = new Get(Bytes.toBytes("3"));
			get3.addColumn(PERSONAL_CF, NAME_COLUMN);
			
			// Do Get 
			getList.add(get2);
			getList.add(get3);
			Result[] results = table.get(getList);
			
			for (Result nextResult : results) {
				nameValue = nextResult.getValue(PERSONAL_CF, NAME_COLUMN);
				logger.info("Name=" + Bytes.toString(nameValue));
			}
			
		
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
		}
		
	}
	
	@Test
	@Ignore
	public void testScanTable() throws Exception {
		
		logger.info("Scan Table Test");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = null;
		// Implements Iterable<Result> means we can use forEach Loop
		ResultScanner scanResult = null;
		try {
			table = connection.getTable(TableName.valueOf("census2"));
			
			Scan scan = new Scan();
			scanResult = table.getScanner(scan);
			for (Result nextResult : scanResult) {
				
				logger.info("OUTER LOOP");
				
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
	public void testDeleteCellData() throws Exception {
		
		logger.info("Test Delete Cell Data");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// USE Case:
		// Delete cell data for RowKey=1
		// delete 'census2', 1, 'personal:gender' 
		// delete 'census2', 1, 'professional:field' 
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf("census2"));
			Delete delete = new Delete(Bytes.toBytes("1"));
			delete.addColumn(PERSONAL_CF, GENDER_COLUMN);
			delete.addColumn(PROFESSIONAL_CF, FIELD_COLUMN);
			table.delete(delete);
			logger.info("==== DELETE COMPLETED");
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
			if (table != null) {
				table.close();
			}
		}
		
	}
	
	@Test
	// @Ignore
	public void testDisableAndDrop() throws Exception {
		
		logger.info("Test Delete Cell Data");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "captain");
		conf.set("hbase.zookeeper.quorum", "captain,godzilla,darwin");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		Connection connection = ConnectionFactory.createConnection(conf);
		
		// USE Case:
		// disable 'census2'
		// drop 'census2'
		// Disable: remove data from memory, remove indexes
		try {
			Admin admin = connection.getAdmin();
			TableName tableName = TableName.valueOf("census2");
			if (admin.tableExists(tableName)) {
				logger.info("Table Exists..deleting");
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				logger.info("Table Deleted");
			} else {
				logger.info("Table Does not Exist");
			}

		} catch (Exception e) {
			logger.error("ERROR", e);
		} finally {
			connection.close();
		}
		
	}

	
}
