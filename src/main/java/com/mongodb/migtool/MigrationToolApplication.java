package com.mongodb.migtool;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Sorts;
import com.mongodb.migtool.util.MigUtil;


@SpringBootApplication
// 자동으로 MongoAutoConfiguration 이나 MongoDataAutoConfiguration 이 실행 되지 않도록 방지
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@EnableConfigurationProperties
public class MigrationToolApplication {
	
	@Value("${dbinfo.oracleip}") String oracleip;
	@Value("${dbinfo.oracleport}") String oracleport;
	@Value("${dbinfo.oracleid}") String oracleid;
	@Value("${dbinfo.oraclepw}") String oraclepw; 
	@Value("${dbinfo.oracledatabasename}") String oracledatabasename;
	@Value("${dbinfo.mongouri}") String mongouri ;
	@Value("${dbinfo.mongodatabasename}") String mongodatabasename ; 
	@Value("${dbinfo.mongodatabaseargs}") String mongodatabaseargs ; 

	private static ArrayList<Map<String,String>> tableList = new ArrayList<Map<String,String>>();
	private static Logger logger = LoggerFactory.getLogger(MigrationToolApplication.class);
	private static MongoClient mongoClient;
	private static Connection conn;
	private static String MONGO_DATABASE_NAME;
	
	public static void main(String[] args) {
		SpringApplication.run(MigrationToolApplication.class, args);
	}
	
	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			logger.info("--------------------------------------------------------------------------------");
			logger.info("Start MigrationToolApplication");
			logger.info("--------------------------------------------------------------------------------");

			System.out.println("******************************************** ");
		    System.out.println("***** application.yml properties info  ***** ");
			Map<String,String> connectionParam = new HashMap<String,String>();
			connectionParam.put("oracleip", oracleip);
			connectionParam.put("oracleport", oracleport);
			connectionParam.put("oracleid", oracleid);
			connectionParam.put("oraclepw", oraclepw);
			connectionParam.put("oracledatabasename", oracledatabasename);
			connectionParam.put("mongouri", mongouri);
			connectionParam.put("mongodatabasename", mongodatabasename);
			connectionParam.put("mongodatabaseargs", mongodatabaseargs);
			System.out.println("ORACLE_IP : "+oracleip);
			System.out.println("ORACLE_PORT : "+oracleport);
			System.out.println("ORACLE_ID : "+oracleid);
			System.out.println("ORACLE_PW : "+oraclepw);
			System.out.println("ORACLE_DATABASE_NAME : "+oracledatabasename);
			System.out.println("ORACLE_MONGO_URI : "+mongouri);
			System.out.println("ORACLE_MONGO_DATABASE : "+ mongodatabasename);
			System.out.println("ORACLE_MONGO_URI_PARAM : "+ mongodatabaseargs);
			System.out.println("***** application.yml properties info ***** ");
			System.out.println("******************************************** ");
			MONGO_DATABASE_NAME = mongodatabasename;
			
			batchInitConnection(connectionParam);
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

	
	private static void oracleToMongoInsert(String tableName , String whereQuery , String insertMongoCollectionName) throws Exception {
		StringBuilder sqlString = new StringBuilder()
				.append("SELECT * FROM  ").append(tableName).append(" where 1=1 and ").append(whereQuery);
		try {

			PreparedStatement psmt = conn.prepareStatement(sqlString.toString());
			// TODO : Set Fetch Count or Limit 1000 Loops
			ResultSet rs = psmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();

			int columnCount = rsmd.getColumnCount();
			int insertCount = 0;

			HashMap buffer = null;

			MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DATABASE_NAME);
			MongoCollection mongoCollection = mongoDatabase.getCollection(insertMongoCollectionName); //"test_collection"
			
			BulkWriteResult bulkWriteResult = null;

			InsertOneModel insertOneModel = null;
			Document insertDoc = null;
			List<InsertOneModel> insertDocuments = new ArrayList<>();

			while (rs.next()) {
				buffer = new HashMap<String,Object>();
				for (int idx = 1; idx < columnCount+1; idx++) {
					String key = rsmd.getColumnName(idx).toUpperCase();
					int columnTypeCode =  rsmd.getColumnType(idx) ;
				    buffer.put(key,  MigUtil.converTypeCasting( columnTypeCode , rs.getObject(idx)));
				}
				Document remappingMap = convertTargetDocumentVO(buffer,insertMongoCollectionName , mongoDatabase );
				insertOneModel = new InsertOneModel(remappingMap);
				insertDocuments.add(insertOneModel);
				insertCount++;
				if(insertCount % 1000 == 0) {
					System.out.println("bulkWriteCount : "+insertCount);
					bulkWriteResult = mongoCollection.bulkWrite(insertDocuments);
					insertDocuments = new ArrayList<>();
				}
			} // End of While Loop Fetch
			// Commit Rest of Insert Doc
			mongoCollection.bulkWrite(insertDocuments);
			System.out.println("From Oracle TableName : " +tableName + " / To MongoCollection : " + insertMongoCollectionName+ " / insert count : "+insertCount);

		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}

	}
	

	public static void batchWorker() {
		boolean isResult = false;
		try {
			System.out.println();
			for ( Map<String, String> data :  tableList) {
				long beforeTime = System.currentTimeMillis();
				oracleToMongoInsert( data.get("tableName") , data.get("whereQuery") , data.get("targetCollection"));
				long afterTime = System.currentTimeMillis(); 
				long secDiffTime = (afterTime - beforeTime)/1000;
				System.out.println("시간차이(m) : "+secDiffTime);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void batchInitConnection(Map<String, String> connectionParam){

		// ---------------------------------------------------------------------
		// Oracle Connect
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@"+connectionParam.get("oracleip")+":"+connectionParam.get("oracleport")+":"+connectionParam.get("oracledatabasename");
		System.out.println("oracle Url : "+url);
		String user = connectionParam.get("oracleid");
		String pwd = connectionParam.get("oraclepw");
		

		conn = getConnection(driver,url,user,pwd);

		// ---------------------------------------------------------------------
		// MongoDB Connect with MongoDB URI format
		ConnectionString connectionString = new ConnectionString(
				//"mongodb://admin:admin@localhost:27017,localhost:27018,localhost:27019/test?authSource=admin&replicaSet=replset"
				// "mongodb://jaehyun:jaehyun@localhost:27017/"+connectionParam.get("ora")+"?authMechanism=SCRAM-SHA-1"
				connectionParam.get("mongouri") + connectionParam.get("mongodatabasename") + "?"  +  connectionParam.get("mongodatabaseargs")
		);
		
		System.out.println("mongo connection info : "+connectionString);
		
		
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
	
   
	public  void batchInitTargetTableSetting () throws Exception{
		
		ArrayList<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map1 = new HashMap<String, String>();
		map1.put("tableName", "TEST_TABLE");
		map1.put("whereQuery", "SEQ <=1 ");
		map1.put("targetCollection","stage1ProfileInfo");
		list.add(map1);
		
//		Map<String,String> map2 = new HashMap<String, String>();
//		map2.put("tableName", "TEST_TABLE");
//		map2.put("whereQuery", "test_id <=5 ");
//		list.add(map2);
//		
//		Map<String,String> map3 = new HashMap<String, String>();
//		map3.put("tableName", "TEST_TABLE");
//		map3.put("whereQuery", "test_id <=5 ");
//		list.add(map3);
		
		tableList = list;
	}
	
	
	
	
	public static Document convertTargetDocumentVO(HashMap buffer, String insertMongoCollectionName , MongoDatabase mongoDatabase ) {
		
		Document dc = new Document();		
		Map<String,Object> idSet = new HashMap<String,Object>();
		Map<String,Object> savedTimeVal = new HashMap<String,Object>();
		if("stage1ProfileInfo".equals(insertMongoCollectionName)) {
			
			dc.append("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString()));
			dc.append("vin",buffer.get("VIN").toString() );
			dc.append("nadID",buffer.get("NADID").toString());
			dc.append("isSynced", Boolean.FALSE);
			dc.append("createdTime", buffer.get("RGST_DTM").toString() );
			dc.append("seq",buffer.get("SEQ").toString());
			
		}else if("stage2ProfileInfo".equals(insertMongoCollectionName)) {
			
			dc.append("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString()));
			dc.append("profileName",buffer.get("PROFILE_NAME").toString());
			dc.append("profileID",buffer.get("PROFILE_ID").toString());
			dc.append("phoneNum",buffer.get("PHONE_NUM").toString());
			dc.append("vin",buffer.get("VIN").toString());
			dc.append("nadID",buffer.get("NADID").toString());
			dc.append("createdTime",buffer.get("RGST_DTM").toString());  
			dc.append("seq",buffer.get("SEQ").toString());
			// dc.append("",);
			
		}else if("stage1PresentProfileSetup".equals(insertMongoCollectionName)) {
		  
		    idSet.put("profileInfoId", new ObjectId( getMasterObjectId( buffer.get("SEQ").toString() , insertMongoCollectionName , mongoDatabase )) );
		    idSet.put("category", buffer.get("CATEGORY").toString()  );
		    
			
			savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
			savedTimeVal.put("category", buffer.get("UTC_OFFSET").toString()  );
			
			dc.append("_id",idSet);
			dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
			dc.append("savedTime",savedTimeVal);
			dc.append("fileName",buffer.get("FILENAME").toString());
			// dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString());
			dc.append("createdDate",buffer.get("RGST_DTM").toString());
			//dc.append("",);

		
		}else if("stage2PresentProfileSetup".equals(insertMongoCollectionName)) {
	   	   
		    idSet.put("profileInfoId", new ObjectId( getMasterObjectId( buffer.get("SEQ").toString() , insertMongoCollectionName , mongoDatabase )) );
		    idSet.put("category", buffer.get("CATEGORY").toString()  );
		    
			
			savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
			savedTimeVal.put("category", buffer.get("UTC_OFFSET").toString()  );
			
			dc.append("_id",idSet);
			dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
			dc.append("savedTime",savedTimeVal);
			dc.append("fileName",buffer.get("FILENAME").toString());
			dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString());
			dc.append("createdDate",buffer.get("RGST_DTM").toString());
		}else if("stage1ChangedProfileSetup".equals(insertMongoCollectionName)) {

		    idSet.put("profileInfoId", new ObjectId( getMasterObjectId( buffer.get("SEQ").toString() , insertMongoCollectionName  , mongoDatabase )) );
		    idSet.put("category", buffer.get("CATEGORY").toString()  );
		    
			savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
			savedTimeVal.put("category", buffer.get("UTC_OFFSET").toString()  );
			
			dc.append("_id",idSet);
			dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
			dc.append("savedTime",savedTimeVal);
			dc.append("fileName",buffer.get("FILENAME").toString());
			dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString());
			dc.append("createdDate",buffer.get("RGST_DTM").toString());
			
		}else if("stage2ChangedProfileSetup".equals(insertMongoCollectionName)) {
		    idSet.put("profileInfoId", new ObjectId( getMasterObjectId( buffer.get("SEQ").toString() , insertMongoCollectionName , mongoDatabase )) );
		    idSet.put("category", buffer.get("CATEGORY").toString()  );
		    
			savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
			savedTimeVal.put("category", buffer.get("UTC_OFFSET").toString()  );
			
			dc.append("_id",idSet);
			dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
			dc.append("savedTime",savedTimeVal);
			dc.append("fileName",buffer.get("FILENAME").toString());
			dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString());
			dc.append("createdDate",buffer.get("RGST_DTM").toString());

		}else {
			
		}
	
		return dc;
	}

	public static String getMasterObjectId (String seq , String insertMongoCollectionName , MongoDatabase mongoDatabase ) {
		Bson filter = Filters.eq("seq",seq);
		Bson sort = Sorts.descending("seq");
		MongoCollection mongoCollection = null;
		if(insertMongoCollectionName.indexOf("1") > 0) {
			mongoCollection = mongoDatabase.getCollection("stage1ProfileInfo");
		}else {
			mongoCollection = mongoDatabase.getCollection("stage2ProfileInfo");
		}
		FindIterable<Document> resultObj =   mongoCollection.find(filter  ).sort(sort);
		
		
		return  resultObj.first().get("_id").toString();
	}


}