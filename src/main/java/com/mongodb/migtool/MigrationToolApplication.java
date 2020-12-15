package com.mongodb.migtool;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
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
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.mongodb.migtool.model.Stage1ChangedProfileSetup;
import com.mongodb.migtool.model.Stage1PresentProfilesSetup;
import com.mongodb.migtool.model.Stage1ProfileInfo;
import com.mongodb.migtool.model.Stage2ChangedProfileSetup;
import com.mongodb.migtool.model.Stage2PresentProfilesSetup;
import com.mongodb.migtool.model.Stage2ProfileInfo;
import com.mongodb.migtool.util.MigUtil;


@SpringBootApplication
// 자동으로 MongoAutoConfiguration 이나 MongoDataAutoConfiguration 이 실행 되지 않도록 방지
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
public class MigrationToolApplication {

	private static ArrayList<Map<String,String>> tableList = new ArrayList<Map<String,String>>();

	private static Logger logger = LoggerFactory.getLogger(MigrationToolApplication.class);

	private final static SimpleDateFormat stdDate = new SimpleDateFormat("yyyyMMdd");

	private static MongoClient mongoClient;
	private static Connection conn;
	
	private final static String OracleDatabaseName = "xe";
	private final static String MongoDataBaseName = "forum";
	
	
	
	
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

			MongoDatabase mongoDatabase = mongoClient.getDatabase("forum");
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
				Map<String,Object> remappingMap = convertTargetDocumentVO(buffer,insertMongoCollectionName );
				insertDoc = new Document(remappingMap);
				insertOneModel = new InsertOneModel(insertDoc);
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
			for ( Map<String, String> data :  tableList) {
				oracleToMongoInsert( data.get("tableName") , data.get("whereQuery") , data.get("targetCollection"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void batchInitConnection(){

		// ---------------------------------------------------------------------
		// Oracle Connect
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url="jdbc:oracle:thin:@localhost:1521:"+OracleDatabaseName;
		String user="system";
		String pwd="oracle";

		conn = getConnection(driver,url,user,pwd);

		// ---------------------------------------------------------------------
		// MongoDB Connect with MongoDB URI format
		ConnectionString connectionString = new ConnectionString(
				//"mongodb://admin:admin@localhost:27017,localhost:27018,localhost:27019/test?authSource=admin&replicaSet=replset"
				"mongodb://jaehyun:jaehyun@localhost:27017/"+MongoDataBaseName+"?authMechanism=SCRAM-SHA-1"
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
	
	

	public  void batchInitTargetTableSetting () throws Exception{
		
		ArrayList<Map<String,String>> list = new ArrayList<Map<String,String>>();
		Map<String,String> map1 = new HashMap<String, String>();
		map1.put("tableName", "TEST_TABLE");
		map1.put("whereQuery", "test_id <=5 ");
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


	public static void main(String[] args) {
		SpringApplication.run(MigrationToolApplication.class, args);
	}
	
	
	
	public static Map<String,Object> convertTargetDocumentVO(HashMap buffer, String insertMongoCollectionName ) {
		
		System.out.println(getMasterObjectId( buffer.get("SEQ").toString() , insertMongoCollectionName ));
		
		Map<String,Object> remapping = new HashMap<String, Object>();
		ObjectMapper objectMapper = new ObjectMapper();
		
		if("stage1ProfileInfo".equals(insertMongoCollectionName)) {
			Map result = objectMapper.convertValue(Stage1ProfileInfo.builder().profileIndex( Integer.parseInt(buffer.get("PROFILE_INDEX").toString()))
																			  .vin(buffer.get("VIN").toString())
																			  .nadID(buffer.get("NADID").toString())
																			  .isSynced(Boolean.FALSE)
																			  .createdTime(buffer.get("RGST_DTM").toString())
																			  .seq(buffer.get("SEQ").toString())
																			  .build(), Map.class);
			remapping = result;
		}else if("stage2ProfileInfo".equals(insertMongoCollectionName)) {
			Map result = objectMapper.convertValue(Stage2ProfileInfo.builder().profileIndex( Integer.parseInt(buffer.get("PROFILE_INDEX").toString()))
																			  .profileName(buffer.get("PROFILE_NAME").toString())
																			  .profileID(buffer.get("PROFILE_ID").toString())
																			  .phoneNum(buffer.get("PHONE_NUM").toString())
																			  .vin(buffer.get("VIN").toString())
																			  .nadID(buffer.get("NADID").toString())
																			  .createdTime(buffer.get("RGST_DTM").toString())
																			  .seq(buffer.get("SEQ").toString())
																			  .build(), Map.class);
			remapping = result;
		}else if("stage1PresentProfileSetup".equals(insertMongoCollectionName)) {
			
			Map result = objectMapper.convertValue(Stage1PresentProfilesSetup.builder()._id(  Stage1PresentProfilesSetup.ProfileInfo.builder().profileInfoId( getMasterObjectId( buffer.get("seq").toString() , insertMongoCollectionName ) ).category(buffer.get("CATEGORY").toString()).build()  )
																					   .metaDataVersion(buffer.get("METADATA_VERSION").toString())
																					   .savedTime(Stage1PresentProfilesSetup.SaveTimeInfo.builder().utc(buffer.get("UTC_OFFSET_DATETIME").toString()).offset(buffer.get("UTC_OFFSET").toString()).build() )
																					   .fileName(buffer.get("FILENAME").toString())		
																					   .setupFile(  !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString()) // category 50번 아닌것 base64 decoding 
																					   .createdDate(buffer.get("RGST_DTM").toString())
																					   .build(),Map.class);
			remapping = result;
		}else if("stage2PresentProfileSetup".equals(insertMongoCollectionName)) {
			
			Map result = objectMapper.convertValue(Stage2PresentProfilesSetup.builder()._id(  Stage2PresentProfilesSetup.ProfileInfo.builder().profileInfoId( getMasterObjectId( buffer.get("seq").toString() , insertMongoCollectionName ) ).category(buffer.get("CATEGORY").toString()).build()  )
																					   .metaDataVersion(buffer.get("METADATA_VERSION").toString())
																					   .savedTime(Stage2PresentProfilesSetup.SaveTimeInfo.builder().utc(buffer.get("UTC_OFFSET_DATETIME").toString()).offset(buffer.get("UTC_OFFSET").toString()).build() )
																					   .fileName( buffer.get("FILENAME").toString())  	
																					   .setupFile(  !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString()) // category 50번 아닌것 base64 decoding 
																					   .createdDate(buffer.get("RGST_DTM").toString())
																					   .build(),Map.class);
			remapping = result;
		}else if("stage1ChangedProfileSetup".equals(insertMongoCollectionName)) {
			
			Map result = objectMapper.convertValue(Stage1ChangedProfileSetup.builder()._id(  Stage1ChangedProfileSetup.ProfileInfo.builder().profileInfoId( getMasterObjectId( buffer.get("seq").toString() , insertMongoCollectionName ) ).category(buffer.get("CATEGORY").toString()).build()  )
																					   .metaDataVersion(buffer.get("METADATA_VERSION").toString())
																					   .savedTime(Stage1ChangedProfileSetup.SaveTimeInfo.builder().utc(buffer.get("UTC_OFFSET_DATETIME").toString()).offset(buffer.get("UTC_OFFSET").toString()).build() )
																					   .fileName( buffer.get("FILENAME").toString())  	
																					   .setupFile(  !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString()) // category 50번 아닌것 base64 decoding 
																					   .createdDate(buffer.get("RGST_DTM").toString())
																					   .build(),Map.class);
			remapping = result;
		}else if("stage2ChangedProfileSetup".equals(insertMongoCollectionName)) {
			Map result = objectMapper.convertValue(Stage2ChangedProfileSetup.builder()._id(  Stage2ChangedProfileSetup.ProfileInfo.builder().profileInfoId( getMasterObjectId( buffer.get("seq").toString() , insertMongoCollectionName ) ).category(buffer.get("CATEGORY").toString()).build()  )
																					   .metaDataVersion(buffer.get("METADATA_VERSION").toString())
																					   .savedTime(Stage2ChangedProfileSetup.SaveTimeInfo.builder().utc(buffer.get("UTC_OFFSET_DATETIME").toString()).offset(buffer.get("UTC_OFFSET").toString()).build() )
																					   .fileName( buffer.get("FILENAME").toString())  	
																					   .setupFile(  !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("FILENAME").toString())  :  buffer.get("FILENAME").toString()) // category 50번 아닌것 base64 decoding 
																					   .createdDate(buffer.get("RGST_DTM").toString())
																					   .build(),Map.class);
		}else {
			
		}
	
		return remapping;
	}

	public static ObjectId getMasterObjectId (String seq , String insertMongoCollectionName ) {
		Bson filter = Filters.eq("seq",seq);
		Bson sort = Sorts.descending("seq");
		
		MongoDatabase mongoDatabase = mongoClient.getDatabase(MongoDataBaseName);
		MongoCollection mongoCollection = null;
	
		if(insertMongoCollectionName.indexOf("1") > 0) {
			mongoCollection = mongoDatabase.getCollection("stage1ProfileInfo");
		}else {
			mongoCollection = mongoDatabase.getCollection("stage2ProfileInfo");
		}
		FindIterable<Document> resultObj =   mongoCollection.find(filter  ).sort(sort);
		return new ObjectId( resultObj.first().get("_id").toString() );
	}


}