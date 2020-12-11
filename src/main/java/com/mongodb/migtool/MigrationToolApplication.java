package com.mongodb.migtool;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;

@SpringBootApplication
// 자동으로 MongoAutoConfiguration 이나 MongoDataAutoConfiguration 이 실행 되지 않도록 방지
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class MigrationToolApplication {

	private static ArrayList<Map<String,String>> tableList = new ArrayList<Map<String,String>>();

	private static Logger logger = LoggerFactory.getLogger(MigrationToolApplication.class);

	private final static SimpleDateFormat stdDate = new SimpleDateFormat("yyyyMMdd");

	private static MongoClient mongoClient;
	private static Connection conn;

	
	



	
	
	private static void oracleToMongoInsert(String tableName , String whereQuery) throws Exception {

		StringBuilder sqlString = new StringBuilder()
				.append("SELECT * FROM  ").append(tableName).append(" WHERE test_id <= 5 ");
		

		try {

			PreparedStatement psmt = conn.prepareStatement(sqlString.toString());
			// TODO : Set Fetch Count or Limit 1000 Loops
			ResultSet rs = psmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();

			int columnCount = rsmd.getColumnCount();
			int insertCount = 0;

			HashMap buffer = null;

			MongoDatabase mongoDatabase = mongoClient.getDatabase("forum");
			MongoCollection mongoCollection = mongoDatabase.getCollection("test_collection");

			BulkWriteResult bulkWriteResult = null;

			InsertOneModel insertOneModel = null;
			Document insertDoc = null;
			List<InsertOneModel> insertDocuments = new ArrayList<>();

			while (rs.next()) {
				buffer = new HashMap<String,Object>();
				for (int idx = 1; idx < columnCount+1; idx++) {
					
					String key = rsmd.getColumnName(idx).toLowerCase();
					int columnTypeCode =  rsmd.getColumnType(idx) ;
				    buffer.put(key, converTypeCasting( columnTypeCode , rs.getObject(idx)));
				}

				insertDoc = new Document(buffer);
				insertOneModel = new InsertOneModel(insertDoc);
				insertDocuments.add(insertOneModel);
				insertCount++;

				if(insertCount % 1000 == 0) {
					System.out.println(insertCount);
					bulkWriteResult = mongoCollection.bulkWrite(insertDocuments);
					insertDocuments = new ArrayList<>();
				}

			} // End of While Loop Fetch

			// Commit Rest of Insert Doc
		
			mongoCollection.bulkWrite(insertDocuments);

		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}

	}

	private static Object converTypeCasting(int columnTypeCode, Object object) throws Exception {
		Object value ;
		if( columnTypeCode == 2005 ){  // clob 2005
	       value = clobToString( (Clob) object );
		}else if( columnTypeCode == 93 )  {
		  DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		  value = df.format( object );
	    
		}else if( columnTypeCode == 2 ) {
			value = Integer.valueOf(((BigDecimal) object).intValue());
		
		}else {
			value =  object;
		}
		return value;
	}

	public static void batchWorker() {
		boolean isResult = false;

		try {
			for ( Map<String, String> data :  tableList) {
				oracleToMongoInsert( data.get("tableName") , data.get("whereQuery"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void batchInitConnection(){

		// ---------------------------------------------------------------------
		// Oracle Connect
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url="jdbc:oracle:thin:@localhost:1521:xe";
		String user="system";
		String pwd="oracle";

		conn = getConnection(driver,url,user,pwd);

		// ---------------------------------------------------------------------
		// MongoDB Connect with MongoDB URI format
		ConnectionString connectionString = new ConnectionString(
				//"mongodb://admin:admin@localhost:27017,localhost:27018,localhost:27019/test?authSource=admin&replicaSet=replset"
				"mongodb://jaehyun:jaehyun@localhost:27017/forum?authMechanism=SCRAM-SHA-1"
		);
		

		// POJO Object를 등록
		CodecRegistry pojoCodecRegistry = fromProviders(
				PojoCodecProvider.
						builder().
						// Add Custom POJO Packages
						//register("").
						build()

		);

		CodecRegistry codecRegistry = fromRegistries(
				MongoClientSettings.getDefaultCodecRegistry(),
				pojoCodecRegistry
		);

		// MongoDB Connection String 과 POJO Object 등의 옵션을 MongoClientSettings 에 적용
		MongoClientSettings mongoClientSettings =
				MongoClientSettings.builder()
						.applyConnectionString(connectionString)
						.codecRegistry(codecRegistry)
						.build();

		// 전역 객체에 MongoDB Client 접속
		mongoClient = MongoClients.create(mongoClientSettings);

	}

	public static Connection getConnection(String driver, String url, String user, String pwd) {
		Connection conn = null;

		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, pwd);
		} catch (SQLException se) {
			String msg = se.getMessage();
			logger.error(msg, se);
			se.printStackTrace();
		} catch (Exception e) {
			String msg = e.getMessage();
			logger.error(msg, e);
			e.printStackTrace();;
		}

		return conn;
	}
	
	public String base64Decoding() {
		return "";
	}
	
	
	public void oracleDummyData ()  throws Exception{
		
		for (int i = 0 ; i < 1000000 ; i++) {
			// Make Insert Query
			String clobValue = "test";
			String strQuery = "INSERT INTO TEST_TABLE ( TEST_ID , TEST_TITLE , TEST_DATE  ) VALUES( ?, ? ,? )";
			PreparedStatement pstmt = conn.prepareStatement(strQuery);
			pstmt.setInt(1, 1);
			// pstmt.setString(2, "test");
			pstmt.setCharacterStream(2 , new StringReader(clobValue), clobValue.length());
			pstmt.setString(3, "sysdate");
			// Insert Row
			int nRowCnt = pstmt.executeUpdate(strQuery);
			//System.out.println(nRowCnt);
			pstmt.close();
			
		}

	}

	public  void batchInitTargetTableSetting () throws Exception{
		
		ArrayList<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map = new HashMap<String, String>();
		map.put("tableName", "TEST_TABLE");
		map.put("whereQuery", "where test_id <=5 ");
		list.add(map);
		tableList = list;
	}
	
	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			logger.info("--------------------------------------------------------------------------------");
			logger.info("Start MigrationToolApplication");
			logger.info("--------------------------------------------------------------------------------");

			
			batchInitConnection();
			batchInitTargetTableSetting();
			// oracleDummyData();
			batchWorker();

			logger.info("--------------------------------------------------------------------------------");
			logger.info("MigrationToolApplication Finished");
			logger.info("--------------------------------------------------------------------------------");

			conn.close();
			mongoClient.close();

		};
	}
	

	public static String clobToString(Clob clob) throws SQLException, IOException {

		if (clob == null) {
			return "";
		}

		StringBuffer strOut = new StringBuffer();

		String str = "";

		BufferedReader br = new BufferedReader(clob.getCharacterStream());

		while ((str = br.readLine()) != null) {
			strOut.append(str);
		}
		return strOut.toString();
	}

	public static void main(String[] args) {
		SpringApplication.run(MigrationToolApplication.class, args);
	}

	
//  colomun type code	
	
//	Array: 2003
//
//	Big int: -5
//
//	Binary: -2
//
//	Bit: -7
//
//	Blob: 2004
//
//	Boolean: 16
//
//	Char: 1
//
//	Clob: 2005
//
//	Date: 91
//
//	Datalink70
//
//	Decimal: 3
//
//	Distinct: 2001
//
//	Double: 8
//
//	Float: 6
//
//	Integer: 4
//
//	JavaObject: 2000
//
//	Long var char: -16
//
//	Nchar: -15
//
//	NClob: 2011
//
//	Varchar: 12
//
//	VarBinary: -3
//
//	Tiny int: -6
//
//	Time stamt with time zone: 2014
//
//	Timestamp: 93
//
//	Time: 92
//
//	Struct: 2002
//
//	SqlXml: 2009
//
//	Smallint: 5
//
//	Rowid: -8
//
//	Refcursor: 2012
//
//	Ref: 2006
//
//	Real: 7
//
//	Nvarchar: -9
//
//	Numeric: 2
//
//	Null: 0
//
//	Smallint: 5
}