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

import javax.annotation.PostConstruct;

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
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.migtool.util.MigUtil;

import lombok.extern.slf4j.Slf4j;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;




@SpringBootApplication
// 자동으로 MongoAutoConfiguration 이나 MongoDataAutoConfiguration 이 실행 되지 않도록 방지
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@EnableConfigurationProperties
@Slf4j
public class MigrationToolApplication implements CommandLineRunner {
	
	@Value("${dbinfo.oracleip}") String oracleip;
	@Value("${dbinfo.oracleport}") String oracleport;
	@Value("${dbinfo.oracleid}") String oracleid;
	@Value("${dbinfo.oraclepw}") String oraclepw; 
	@Value("${dbinfo.oracledatabasename}") String oracledatabasename;
	@Value("${dbinfo.mongouri}") String mongouri ;
	@Value("${dbinfo.mongodatabasename}") String mongodatabasename ; 
	@Value("${dbinfo.mongodatabaseargs}") String mongodatabaseargs ; 
	@Value("${dbinfo.fromRowNum}") String fromRowNum ; 
	@Value("${dbinfo.toRowNum}") String toRowNum ; 
	@Value("${dbinfo.servicetype}") String servicetype ; 
	@Value("${dbinfo.fromUptDtm}") String fromUptDtm ;
	@Value("${dbinfo.toUptDtm}") String toUptDtm ;
	@Value("${dbinfo.infoTableFromSeq}") String infoFromSeq ;
	@Value("${dbinfo.infoTableToSeq}") String infoToSeq ;
	@Value("${dbinfo.bulkUpdateYn}") String bulkUpdateYn ;

	private static ArrayList<Map<String,String>> tableList = new ArrayList<Map<String,String>>();
	private static Logger logger = LoggerFactory.getLogger(MigrationToolApplication.class);
	private static MongoClient mongoClient;
	private static Connection conn;
	private static String MONGO_DATABASE_NAME;
	private static String SERVICE_TYPE;
	private static String FROM_ROWNUM;
	private static String TO_ROWNUM;
	private static String FROM_UPT_DTM;
	private static String TO_UPT_DTM;
	private static String INFO_FROMSEQ;
	private static String INFO_TOSEQ;
	private static String BULK_UPDATE_YN;
	private static Map<String,String> CONNECTIONPARAM = new HashMap<String,String>();
	
	public static void main(String[] args) {
		
		/* java -jar -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="1000" migtool-0.0.1-SNAPSHOT.jar    */ 
		SpringApplication.run(MigrationToolApplication.class, args);
	}
	
	
	@PostConstruct
	public void init() {
		logger.info("-----------------test---------------------------------------------------------------");
	}
	
	@Override
    public void run(String... args) throws Exception {
		logger.info("--------------------------------------------------------------------------------");
		logger.info("Start MigrationToolApplication");
		logger.info("--------------------------------------------------------------------------------");
	
		logger.info("******************************************** ");
	    logger.info("***** application.yml properties info  ***** ");
		Map<String,String> connectionParam = new HashMap<String,String>();
		connectionParam.put("oracleip", oracleip);
		connectionParam.put("oracleport", oracleport);
		connectionParam.put("oracleid", oracleid);
		connectionParam.put("oraclepw", oraclepw);
		connectionParam.put("oracledatabasename", oracledatabasename);
		connectionParam.put("mongouri", mongouri);
		connectionParam.put("mongodatabasename", mongodatabasename);
		connectionParam.put("mongodatabaseargs", mongodatabaseargs);
		connectionParam.put("fromUptDtm", fromUptDtm);
		connectionParam.put("toUptDtm", toUptDtm);
		connectionParam.put("infoFromSeq", infoFromSeq);
		connectionParam.put("infoToSeq", infoToSeq);
		connectionParam.put("bulkUpdateYn", bulkUpdateYn);
		logger.info("dbinfo.oracleip : "+oracleip);
		logger.info("dbinfo.oracleport : "+oracleport);
		logger.info("dbinfo.oracleid : "+oracleid);
		logger.info("dbinfo.oraclepw : ["+oraclepw+"]");
		logger.info("dbinfo.oracledatabasename : "+oracledatabasename);
		logger.info("dbinfo.mongouri : "+mongouri);
		logger.info("dbinfo.mongodatabasename : "+ mongodatabasename);
		logger.info("dbinfo.mongodatabaseargs : "+ mongodatabaseargs);
		logger.info("dbinfo.servicetype : "+ servicetype);
		logger.info("dbinfo.fromUptDtm : "+ fromUptDtm);
		logger.info("dbinfo.toUptDtm : "+ toUptDtm);
		logger.info("dbinfo.infoFromSeq : "+ infoFromSeq);
		logger.info("dbinfo.infoToSeq : "+ infoToSeq);
		logger.info("dbinfo.bulkUpdateYn : "+ bulkUpdateYn);
	
		logger.info("***** application.yml properties info ***** ");
		logger.info("******************************************** ");
		logger.info("");
		logger.info(" >> Service Type :  "+servicetype);
		logger.info(" >> From DB RowNum  :  "+fromRowNum);
		logger.info(" >> To DB RowNum    :  "+toRowNum);
		logger.info(" >> fromUptDtm   :  "+fromUptDtm);
		logger.info(" >> toUptDtm   :  "+toUptDtm);
		logger.info(" >> infoFromSeq   :  "+infoFromSeq);
		logger.info(" >> infoToSeq   :  "+infoToSeq);
		logger.info(" >> bulkUpdateYn   :  "+bulkUpdateYn);
		
		logger.info("");
		logger.info("******************************************** ");
		
		MONGO_DATABASE_NAME = mongodatabasename;
		FROM_ROWNUM=fromRowNum;
		TO_ROWNUM=toRowNum;
		FROM_UPT_DTM = fromUptDtm;
		TO_UPT_DTM = toUptDtm;
		INFO_FROMSEQ = infoFromSeq;
		INFO_TOSEQ = infoToSeq;
		SERVICE_TYPE = servicetype;
		CONNECTIONPARAM = connectionParam;
		batchInitConnection(connectionParam);
		batchInitTargetTableSetting();
		// oracleDummyData();
		batchWorker();

		logger.info("--------------------------------------------------------------------------------");
		logger.info("MigrationToolApplication Finished");
		logger.info("--------------------------------------------------------------------------------");

		conn.close();
		mongoClient.close();

	}
	

	


	
	
	private static void oracleToMongoInsert(String tableName , String whereQuery , String insertMongoCollectionName , String betweenQuery , String betweenA , String betweenB  ) throws Exception {
		StringBuilder sqlString = new StringBuilder()
				.append("SELECT S.* FROM  ").append(tableName).append(" where 1=1 and ").append(whereQuery).append(  betweenQuery ).append(" ").append(betweenA).append(" AND ").append(betweenB);
		
		
		StringBuilder sqlStringCNT = new StringBuilder()
				.append("SELECT COUNT(*) FROM  ").append(tableName).append(" where 1=1 and ").append(whereQuery).append( betweenQuery ).append(" ").append(betweenA).append(" AND ").append(betweenB);
		
		
		try {
			logger.info("Fetch Execute Query :: "+sqlString.toString());
			logger.info("Fetch Execute Query CNT :: "+sqlStringCNT.toString());
			PreparedStatement psmt = conn.prepareStatement(sqlString.toString());
			PreparedStatement psmtCnt = conn.prepareStatement(sqlStringCNT.toString());
			//PreparedStatement psmtCnt = conn.prepareStatement(sqlCnt.toString());
			// TODO : Set Fetch Count or Limit 1000 Loops
			ResultSet rs = psmt.executeQuery();
			ResultSet rsCnt = psmtCnt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rsCnt.next()) {
				logger.info("Fetch Cnt : "+ rsCnt.getObject(1));
			}
			
			int columnCount = rsmd.getColumnCount();
			int insertCount = 0;
			int updateCount = 0;

			HashMap buffer = null;

			MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DATABASE_NAME);
			MongoCollection mongoCollection = mongoDatabase.getCollection(insertMongoCollectionName); //"test_collection"
			
			BulkWriteResult bulkWriteResult = null;

			InsertOneModel insertOneModel = null;
			Document insertDoc = null;
			List<InsertOneModel> insertDocuments = new ArrayList<>();
			UpdateOneModel updateOneModel = null;
			List<UpdateOneModel> updateDocuments = new ArrayList<>();
			
			int lastRowNum = 0;
			int bulkInsertCount = 0;
			int bulkUpdateCount = 0;
			
			while (rs.next()) {
				
				buffer = new HashMap<String,Object>();
				for (int idx = 1; idx < columnCount+1; idx++) {
					String key = rsmd.getColumnName(idx).toUpperCase();
					int columnTypeCode =  rsmd.getColumnType(idx) ;
				    buffer.put(key,  MigUtil.converTypeCasting( columnTypeCode , rs.getObject(idx)));
				}
				Document remappingMap = convertTargetDocumentVO(buffer,insertMongoCollectionName , mongoDatabase );
				if( ! remappingMap.isEmpty() ) {
					// lastRowNum = Integer.parseInt(remappingMap.get("seq").toString());
					if(!insertDocuments.contains(remappingMap)) {
						if("N".equals(BULK_UPDATE_YN)) {
							insertOneModel = new InsertOneModel(remappingMap);
							insertDocuments.add(insertOneModel);
							insertCount++;	
						}else {
							if("stage1ProfileInfo".equals(insertMongoCollectionName) ) {
								Bson filter = and (eq("vin",buffer.get("VIN").toString()),eq("nadId",buffer.get("NADID").toString()),eq("profileIndex", Integer.parseInt(buffer.get("PROFILE_INDEX").toString())));
								updateOneModel = new UpdateOneModel( filter , new Document("$set" , remappingMap ) , new UpdateOptions().upsert(true) );
								updateDocuments.add(updateOneModel);
								updateCount++;
							}else if( "stage2ProfileInfo".equals(insertMongoCollectionName)) {
								Bson filter = and ( eq("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString())), eq("profileID",buffer.get("PROFILE_ID").toString()) , eq("vin",buffer.get("VIN").toString()) ,  eq("nadId",buffer.get("NADID").toString()));
								updateOneModel = new UpdateOneModel( filter , new Document("$set" , remappingMap ) , new UpdateOptions().upsert(true) );
								updateDocuments.add(updateOneModel);
								updateCount++;
							}else if ("stage1PresentProfileSetup".equals(insertMongoCollectionName)) {
								Bson filter = Filters.eq("seq",buffer.get("INFO_SEQ").toString());
								Bson sort = Sorts.descending("seq");
								MongoCollection stage1ProfileInfo = mongoDatabase.getCollection("stage1ProfileInfo");
								logger.info("stage1ProfileSetup Search seq : "+buffer.get("INFO_SEQ").toString());
								FindIterable<Document> resultObj =   stage1ProfileInfo.find( filter  ).sort(sort);
							    
								if(resultObj.first() !=null) {
									Map<String,Object> map = new HashMap<String,Object>();
									String mongoInfoObjectId =  resultObj.first().get("_id").toString();
									map.put("profileInfoId", new ObjectId(mongoInfoObjectId) );
									map.put("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE  );
									map.put("category", buffer.get("CATEGORY").toString()  );
									
									
									Bson searchProfileSetupObjectId = Filters.eq("_id",map);
									updateOneModel = new UpdateOneModel( filter , new Document("$set" , remappingMap ) , new UpdateOptions().upsert(true) );
									updateDocuments.add(updateOneModel);
									updateCount++;
								}	
							}else if ("stage2PresentProfileSetup".equals(insertMongoCollectionName)) {
								Bson filter = Filters.eq("seq",buffer.get("INFO_SEQ").toString());
								Bson sort = Sorts.descending("seq");
								MongoCollection stage2ProfileInfo = mongoDatabase.getCollection("stage2ProfileInfo");
								logger.info("stage2ProfileSetup Search seq : "+buffer.get("INFO_SEQ").toString());
								FindIterable<Document> resultObj =   stage2ProfileInfo.find( filter  ).sort(sort);
							    
								if(resultObj.first() !=null) {
									Map<String,Object> map = new HashMap<String,Object>();
									String mongoInfoObjectId =  resultObj.first().get("_id").toString();
									map.put("profileInfoId", new ObjectId(mongoInfoObjectId) );
									map.put("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE  );
									// map.put("category", buffer.get("CATEGORY").toString()  );
									
									
									Bson searchProfileSetupObjectId = Filters.eq("_id",map);
									updateOneModel = new UpdateOneModel( filter , new Document("$set" , remappingMap ) , new UpdateOptions().upsert(true) );
									updateDocuments.add(updateOneModel);
									updateCount++;
								}	
							}
						}
						
					}
				}
				if(insertCount % 1000 == 0    ) {
					logger.info("bulkWrite Insert Count : "+insertCount);
					if(insertDocuments.size() !=0) {
						bulkWriteResult = mongoCollection.bulkWrite(insertDocuments);
						insertDocuments = new ArrayList<>();
						bulkInsertCount++;
					}
				}
				if(updateCount % 1000 == 0    ) {
					logger.info("bulkWrite Update Count : "+updateCount);
					if(updateDocuments.size() !=0) {
						bulkWriteResult = mongoCollection.bulkWrite(updateDocuments);
						updateDocuments = new ArrayList<>();
						bulkUpdateCount++;
					}
				}
			} // End of While Loop Fetch
			if(insertDocuments.size() > 0) {
				bulkWriteResult = mongoCollection.bulkWrite(insertDocuments);
			}
			
			if(updateDocuments.size() > 0) {
				bulkWriteResult = mongoCollection.bulkWrite(updateDocuments);
			}
		
			logger.info("From Oracle TableName : " +tableName + " / To MongoCollection : " + insertMongoCollectionName+ " / insert count : "+insertCount + " / update count : "+updateCount );
		
			
			

		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}

	}
	

	public static void batchWorker() {
		boolean isResult = false;
		try {
			
			
			for ( Map<String, String> data :  tableList) {
				long beforeTime = System.currentTimeMillis();
				
				
				
				float fromRow = Integer.parseInt(data.get("fromRow").toString());
				float toRow = Integer.parseInt(data.get("toRow").toString());
				
				int step = 10000;
				String str = String.format("%.2f",(toRow-fromRow) / new Float(step) );
				int grp = (int) Math.ceil(Double.parseDouble(str));
				
				float betweenA = 0 ;
				float betweenB = 0 ;
				for ( int i = 0 ; i < grp ; i++) {
					betweenA = fromRow + ( i * step );
					betweenB = (betweenA + step ) - 1;
					if( grp ==  i + 1) {
						// System.out.println("between A " + Integer.toString((int) betweenA) + " and "+ Integer.toString((int)toRow));
						oracleToMongoInsert( data.get("tableName") , data.get("whereQuery") , data.get("targetCollection") , data.get("between") ,  Integer.toString((int) betweenA) ,Integer.toString((int)toRow) );
					}else {
						// System.out.println("between A " +  Integer.toString((int) betweenA) + " and "+ Integer.toString((int)betweenB));
						oracleToMongoInsert( data.get("tableName") , data.get("whereQuery") , data.get("targetCollection") , data.get("between") ,  Integer.toString((int) betweenA) , Integer.toString((int)betweenB) );
					}
				}
			
				
				// oracleToMongoInsert( data.get("tableName") , data.get("whereQuery") , data.get("targetCollection") ,  );
				
				long afterTime = System.currentTimeMillis(); 
				long secDiffTime = (afterTime - beforeTime)/1000;
				logger.info("excute runtime(sec) : "+secDiffTime);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void batchInitConnection(Map<String, String> connectionParam) throws SQLException{

		// ---------------------------------------------------------------------
		// Oracle Connect
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@"+connectionParam.get("oracleip")+":"+connectionParam.get("oracleport")+"/"+connectionParam.get("oracledatabasename");
		logger.info(">> Oracle Url : "+url);
		String user = connectionParam.get("oracleid");
		String pwd = connectionParam.get("oraclepw");
		

		conn = getConnection(driver,url,user,pwd);
		// conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

		// ---------------------------------------------------------------------
		// MongoDB Connect with MongoDB URI format
		ConnectionString connectionString = new ConnectionString(
				//"mongodb://admin:admin@localhost:27017,localhost:27018,localhost:27019/test?authSource=admin&replicaSet=replset"
				// "mongodb://jaehyun:jaehyun@localhost:27017/"+connectionParam.get("ora")+"?authMechanism=SCRAM-SHA-1"
				connectionParam.get("mongouri") + connectionParam.get("mongodatabasename") + "?"  +  connectionParam.get("mongodatabaseargs")
		);
		
		logger.info(">> Mongo connection info : "+connectionString);
		
		
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
		if("1".equals(servicetype)) {
			/* master table : start */
			Map<String,String> map1 = new HashMap<String, String>();
			map1.put("tableName", "Profile_Info S ");
			// map1.put("whereQuery", "SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +" ");
			map1.put("whereQuery", " 1=1 ");
			map1.put("targetCollection","stage1ProfileInfo");
			map1.put("between", " AND SEQ BETWEEN ");
			map1.put("fromRow",FROM_ROWNUM);
			map1.put("toRow",TO_ROWNUM);
			list.add(map1);
			
			Map<String,String> map2 = new HashMap<String, String>();
			map2.put("tableName", "Profile_Info S ");
			map2.put("whereQuery", " 1=1 ");
			map2.put("targetCollection","stage2ProfileInfo");
			map2.put("between", " AND INFO_SEQ BETWEEN ");
			map2.put("fromRow",FROM_ROWNUM);
			map2.put("toRow",TO_ROWNUM);
			list.add(map2);
			/* master table end */
		}
		
		if("2".equals(servicetype)) {
			Map<String,String> map3 = new HashMap<String, String>();
			map3.put("tableName", "Profile_Setup_Data S ");
			// map3.put("whereQuery", " INFO_SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +" AND PROFILE_LEVEL = 1  AND TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') <= "+TO_UPT_DTM + "  ");
			map3.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 0 AND TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') < "+TO_UPT_DTM + "  ");
			map3.put("targetCollection","stage1PresentProfileSetup");
			map3.put("between", " AND INFO_SEQ BETWEEN");
			map3.put("fromRow", FROM_ROWNUM);
			map3.put("toRow", TO_ROWNUM);
			list.add(map3);
		}

		if("3".equals(servicetype)) {
			Map<String,String> map4 = new HashMap<String, String>();
			map4.put("tableName", "Profile_Setup_Data S ");
			map4.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 1 AND UPT_DTM IS NULL ");
			map4.put("targetCollection","stage1PresentProfileSetup");
			map4.put("between", " AND INFO_SEQ BETWEEN ");
			map4.put("fromRow",FROM_ROWNUM);
			map4.put("toRow",TO_ROWNUM);
			list.add(map4);
		}
		
		if("4".equals(servicetype)) {
			Map<String,String> map4 = new HashMap<String, String>();
			map4.put("tableName", "Profile_Setup_Data S ");
			//map4.put("whereQuery", " INFO_SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +"  AND PROFILE_LEVEL = 2  AND GUBUN = 1 ");
			map4.put("whereQuery", " PROFILE_LEVEL = 2  AND GUBUN = 1 ");
			map4.put("targetCollection","stage2PresentProfileSetup");
			map4.put("between", " AND INFO_SEQ BETWEEN ");
			map4.put("fromRow",FROM_ROWNUM);
			map4.put("toRow",TO_ROWNUM);
			list.add(map4);
		}
		
		
		// update 구간
		
		if("6".equals(servicetype)) {
			Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
			map3.put("tableName", "Profile_Setup_Data S ");
			map3.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 0 ");
			// SEQ > 20000
			map3.put("targetCollection","stage1PresentProfileSetup");
			map3.put("between", " AND TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') BETWEEN ");
			map3.put("fromRow",FROM_UPT_DTM);
			map3.put("toRow",TO_UPT_DTM);
			list.add(map3);
		}
		
		if("7".equals(servicetype)) {
			// map3.put("whereQuery", " ( INFO_SEQ BETWEEN "+ FROM_ROWNUM + " AND "+ TO_ROWNUM  +" AND  PROFILE_LEVEL = 1 ) or ( GUBUN=0 and TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') BETWEEN  "+ FROM_UPT_DTM +" AND "+ TO_UPT_DTM +"  ) or ( SEQ BETWEEN  "+ INFO_FROMSEQ +" AND "+ INFO_TOSEQ +" ) ");
			Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
			map3.put("tableName", "Profile_Setup_Data S ");
			map3.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 1 AND UPT_DTM IS NULL  " );
		
			// SEQ > 20000
			map3.put("targetCollection","stage1PresentProfileSetup");
			map3.put("between", " AND INFO_SEQ BETWEEN ");
			map3.put("fromRow",FROM_ROWNUM);
			map3.put("toRow",TO_ROWNUM);
			list.add(map3);
		}
		
		
		
		if("8".equals(servicetype)) {
			Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
			map3.put("tableName", "Profile_Setup_Data S ");
			map3.put("whereQuery", " PROFILE_LEVEL = 2  AND GUBUN = 1  ");
			// SEQ > 20000
			map3.put("targetCollection","stage2PresentProfileSetup");
			map3.put("between", " AND SEQ BETWEEN ");
			map3.put("fromRow",INFO_FROMSEQ);
			map3.put("toRow",INFO_TOSEQ);
			list.add(map3);
		}
		
//		if("9".equals(servicetype)) {
//			Map<String,String> map4 = new HashMap<String, String>();
//			map4.put("tableName", "Profile_Setup_Data S ");
//			map4.put("whereQuery", " PROFILE_LEVEL = 2  AND GUBUN = 1 ");
//			map4.put("targetCollection","stage2PresentProfileSetup");
//			map4.put("between", " AND INFO_SEQ BETWEEN");
//			map4.put("fromRow",FROM_ROWNUM);
//			map4.put("toRow",TO_ROWNUM);
//			list.add(map4);
//		}
		
		
	
		
		
		
		
		tableList = list;
	}
	
	
	
	
	public static Document convertTargetDocumentVO(HashMap buffer, String insertMongoCollectionName , MongoDatabase mongoDatabase ) {
		
		Document dc = new Document();		
		Map<String,Object> idSet = new HashMap<String,Object>();
		Map<String,Object> savedTimeVal = new HashMap<String,Object>();
		if("stage1ProfileInfo".equals(insertMongoCollectionName)) {
			
			dc.append("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString()));
			dc.append("vin",buffer.get("VIN").toString() );
			dc.append("nadId",buffer.get("NADID").toString());
			dc.append("isSynced", Boolean.FALSE);
			if (  buffer.get("RGST_DTM") !=null ) {
				dc.append("createTime", buffer.get("RGST_DTM").toString() );  // null check
			}
			dc.append("seq",buffer.get("SEQ").toString());
			
		}else if("stage2ProfileInfo".equals(insertMongoCollectionName)) {
			if( buffer.get("PROFILE_INDEX") !=null) {
				dc.append("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString()));
			}
			if( buffer.get("PROFILE_NAME") !=null) {
				dc.append("profileName",buffer.get("PROFILE_NAME").toString());
			}
			if( buffer.get("PROFILE_ID") !=null ) {
				dc.append("profileID",buffer.get("PROFILE_ID").toString());
			}
			if( buffer.get("PHONE_NUM") !=null ) {
				dc.append("phoneNum",buffer.get("PHONE_NUM").toString()); // null check
			}
			dc.append("vin",buffer.get("VIN").toString());
			dc.append("nadId",buffer.get("NADID").toString());
			if( buffer.get("RGST_DTM") !=null ) {
				dc.append("createTime",buffer.get("RGST_DTM").toString());   // null check
			}
			dc.append("seq",buffer.get("SEQ").toString());
			// dc.append("",);
			
		}else if("stage1PresentProfileSetup".equals(insertMongoCollectionName)) {
			//logger.info("stage1PresentProfileSetup Oracle Search INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
			
		    String masterObId = getMasterObjectId( buffer.get("INFO_SEQ").toString() , insertMongoCollectionName , mongoDatabase );
		    //logger.info("masterObId :"+masterObId);
			if(masterObId !=null) {
				    
					idSet.put("profileInfoId", new ObjectId( masterObId ));
				    idSet.put("category", buffer.get("CATEGORY").toString()  );
				    idSet.put("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE  );
					
					savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
					savedTimeVal.put("offset", buffer.get("UTC_OFFSET").toString()  );
					
					dc.append("_id",idSet);
					dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
					dc.append("savedTime",savedTimeVal);
					dc.append("fileName",buffer.get("FILENAME").toString());
					if(buffer.get("SETUP_FILE")!=null) {
						
						try {
							dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("SETUP_FILE").toString())  :  buffer.get("SETUP_FILE").toString());
						} catch (IllegalArgumentException e) {
							//e.printStackTrace();
							logger.info("stage1PresentProfileSetup IllegalArgumentException INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
							return new Document();		
						} catch (Exception e) {
							logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
							//e.printStackTrace();
							return new Document();		
						}
					}
					if(buffer.get("RGST_DTM")!=null) {
						dc.append("createDate",buffer.get("RGST_DTM").toString());
					}
			}
		
		}else if("stage2PresentProfileSetup".equals(insertMongoCollectionName)) {
			//logger.info("Oracle Search INFO_SEQ :"+ buffer.get("INFO_SEQ").toString());
			String masterObId = getMasterObjectId( buffer.get("INFO_SEQ").toString() , insertMongoCollectionName , mongoDatabase );
		    //logger.info("stage2PresentProfileSetup masterObId :"+masterObId);
			if(masterObId !=null) {
			    idSet.put("profileInfoId", new ObjectId( masterObId ) );
			    idSet.put("category", buffer.get("CATEGORY").toString()  );
				savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
				savedTimeVal.put("offset", buffer.get("UTC_OFFSET").toString()  );
				
				dc.append("_id",idSet);
				dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
				dc.append("savedTime",savedTimeVal);
				dc.append("fileName",buffer.get("FILENAME").toString());
				if(buffer.get("SETUP_FILE")!=null) {
					try {
						dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("SETUP_FILE").toString())  :  buffer.get("SETUP_FILE").toString());
					} catch (IllegalArgumentException e) {
						//e.printStackTrace();
						logger.info("stage1PresentProfileSetup IllegalArgumentException INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						return new Document();		
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
						return new Document();		
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
			}
		}else if("stage1ChangedProfileSetup".equals(insertMongoCollectionName)) {
			String masterObId = getMasterObjectId( buffer.get("INFO_SEQ").toString() , insertMongoCollectionName , mongoDatabase );
			if(masterObId !=null) {
			    idSet.put("profileInfoId", new ObjectId(masterObId) );
			    idSet.put("category", buffer.get("CATEGORY").toString()  );
			    
				savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
				savedTimeVal.put("offset", buffer.get("UTC_OFFSET").toString()  );
				
				dc.append("_id",idSet);
				dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
				dc.append("savedTime",savedTimeVal);
				dc.append("fileName",buffer.get("FILENAME").toString());
				if(buffer.get("SETUP_FILE")!=null) {
					try {
						dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("SETUP_FILE").toString())  :  buffer.get("SETUP_FILE").toString());
					} catch (IllegalArgumentException e) {
						//e.printStackTrace();
						logger.info("stage1PresentProfileSetup IllegalArgumentException INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						return new Document();		
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
						return new Document();		
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
			}
		}else if("stage2ChangedProfileSetup".equals(insertMongoCollectionName)) {
			String masterObId = getMasterObjectId( buffer.get("INFO_SEQ").toString() , insertMongoCollectionName , mongoDatabase );
			if(masterObId !=null) {
			    idSet.put("profileInfoId", new ObjectId(masterObId) );
			    idSet.put("category", buffer.get("CATEGORY").toString()  );
			    
				savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
				savedTimeVal.put("offset", buffer.get("UTC_OFFSET").toString()  );
				
				dc.append("_id",idSet);
				dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
				dc.append("savedTime",savedTimeVal);
				dc.append("fileName",buffer.get("FILENAME").toString());
				
				if(buffer.get("SETUP_FILE")!=null) {
					try {
						dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("SETUP_FILE").toString())  :  buffer.get("SETUP_FILE").toString());
					} catch (IllegalArgumentException e) {
						//e.printStackTrace();
						logger.info("stage1PresentProfileSetup IllegalArgumentException INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						return new Document();		
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
						return new Document();		
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
			}
		}else {
			
		}
	
		return dc;
	}

	public static String getMasterObjectId (String info_seq , String insertMongoCollectionName , MongoDatabase mongoDatabase ) {
		
		Bson filter = Filters.eq("seq",info_seq);
		Bson sort = Sorts.descending("seq");
		MongoCollection mongoCollection = null;
		if(insertMongoCollectionName.indexOf("1") > 0) {
			mongoCollection = mongoDatabase.getCollection("stage1ProfileInfo");
		}else {
			mongoCollection = mongoDatabase.getCollection("stage2ProfileInfo");
		}
		FindIterable<Document> resultObj =   mongoCollection.find(filter  ).sort(sort);
		
		if ( resultObj.first() != null) {
			return resultObj.first().get("_id").toString();
		}else {
			return null;
		}
		
	}
	
	

	public static void updateMongoSync(HashMap buffer , String insertMongoCollectionName , MongoDatabase mongoDatabase  ) {
		Bson filter = Filters.eq("seq",buffer.get("SEQ"));
		Bson sort = Sorts.descending("seq");
		MongoCollection mongoCollection = mongoDatabase.getCollection("stage1ProfileInfo");
		FindIterable<Document> resultObj =   mongoCollection.find(filter  ).sort(sort);
	    
		if(resultObj.first() !=null) {
			Document dc = new Document();
			dc.append("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE );
			mongoCollection.findOneAndUpdate(filter, new Document("$set",dc));
		}
	}
	


}