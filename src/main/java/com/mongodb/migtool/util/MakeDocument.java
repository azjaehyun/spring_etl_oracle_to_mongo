package com.mongodb.migtool.util;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;

public class MakeDocument {
	
	/*
	public static String stage1ProfileInfoCollectionDupCheck(HashMap buffer , String insertMongoCollectionName , MongoDatabase mongoDatabase ) {
		Bson filter = and (eq("vin",buffer.get("VIN").toString()),eq("nadId",buffer.get("NADID").toString()),eq("profileIndex", Integer.parseInt(buffer.get("PROFILE_INDEX").toString())));
		
		MongoCollection mongoCollection = mongoDatabase.getCollection("stage1ProfileInfo");
		FindIterable<Document> resultObj =   mongoCollection.find(  filter  );
		
		Document dc = new Document();
		dc.append("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString()));
		dc.append("vin",buffer.get("VIN").toString() );
		dc.append("nadId",buffer.get("NADID").toString());
		dc.append("isSynced", Boolean.FALSE);
		if (  buffer.get("RGST_DTM") !=null ) {
			dc.append("createTime", buffer.get("RGST_DTM").toString() );  // null check
		}
		dc.append("seq",buffer.get("SEQ").toString());
		
		if ( resultObj.first() != null) {  // 과거 내역 지우고 다시 insert 
			DeleteResult ds = mongoCollection.deleteMany(filter);
			logger.info("deleteCnt : "+ds.getDeletedCount()+ " delete stage1ProfileInfo : VIN : "+buffer.get("VIN").toString()+" NADID : "+buffer.get("NADID").toString());	
		}	
		mongoCollection.insertOne(dc);
		logger.info("insert stage1ProfileInfo : VIN : "+buffer.get("VIN").toString()+" NADID : "+buffer.get("NADID").toString());	
		return "";
	}
	
	public static String stage2ProfileInfoCollectionDupCheck(HashMap buffer , String insertMongoCollectionName , MongoDatabase mongoDatabase ) {
		Bson filter = and (   eq("profileIndex",Integer.parseInt(buffer.get("PROFILE_INDEX").toString())), eq("profileID",buffer.get("PROFILE_ID").toString()) , eq("vin",buffer.get("VIN").toString()) ,  eq("nadId",buffer.get("NADID").toString()));
		MongoCollection mongoCollection = mongoDatabase.getCollection("stage2ProfileInfo");
		FindIterable<Document> resultObj =   mongoCollection.find(  filter  );
		Document dc = new Document();
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
		if ( resultObj.first() != null) {  // 과거 내역 지우고 다시 insert 
			DeleteResult ds = mongoCollection.deleteMany(filter);
			logger.info("deleteCnt : "+ds.getDeletedCount()+ " delete stage2ProfileInfo : PROFILE_INDEX : "+buffer.get("PROFILE_INDEX").toString()+" profileID : "+buffer.get("PROFILE_ID").toString() + "VIN : "+buffer.get("VIN").toString()+" NADID : "+buffer.get("NADID").toString());	
		}	
		logger.info("insert stage2ProfileInfo : PROFILE_INDEX : "+buffer.get("PROFILE_INDEX").toString()+" profileID : "+buffer.get("PROFILE_ID").toString() + "VIN : "+buffer.get("VIN").toString()+" NADID : "+buffer.get("NADID").toString());
		mongoCollection.insertOne(dc);
		return "";
	}
	
	
	
	
	
	
	//TODO
	public static String stage1ProfileSetupCollectionSync( HashMap buffer , String insertMongoCollectionName , MongoDatabase mongoDatabase  ) throws SQLException {
		
		Document dc = new Document();		
		Map<String,Object> idSet = new HashMap<String,Object>();
		Map<String,Object> savedTimeVal = new HashMap<String,Object>();
		
		Bson filter = Filters.eq("seq",buffer.get("INFO_SEQ").toString());
		Bson sort = Sorts.descending("seq");
		MongoCollection mongoCollection = mongoDatabase.getCollection("stage1ProfileInfo");
		logger.info("stage1ProfileSetup Search seq : "+buffer.get("INFO_SEQ").toString());
		FindIterable<Document> resultObj =   mongoCollection.find(filter  ).sort(sort);
	    
		if(resultObj.first() !=null) {
			Map<String,Object> map = new HashMap<String,Object>();
			String mongoInfoObjectId =  resultObj.first().get("_id").toString();
			map.put("profileInfoId", new ObjectId(mongoInfoObjectId) );
			map.put("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE  );
			map.put("category", buffer.get("CATEGORY").toString()  );
			
			
			Bson searchProfileSetupObjectId = Filters.eq("_id",map);
			MongoCollection mongoCollection2 = mongoDatabase.getCollection("stage1PresentProfileSetup");
			logger.info("stage1PresentProfileSetup Search _id : "+map);
			FindIterable<Document> resultObj2  = mongoCollection2.find(searchProfileSetupObjectId);
			if( resultObj2.first() !=null ) {   // 변경된 마스터 아이디를 가져와서~
				// 그냥 무조건 지우고 insert
				//Bson deleteParam = Filters.eq("_id",searchProfileSetupObjectId);
				// mongoCollection2.deleteOne(deleteParam);
			
				UpdateOptions updateOptions = new UpdateOptions().upsert(true);
				
				logger.info("stage1ProfileSetup delete : "+searchProfileSetupObjectId);
				idSet.put("profileInfoId", new ObjectId( mongoInfoObjectId ) );
			    idSet.put("category", buffer.get("CATEGORY").toString()  );
			    idSet.put("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE  );
				
				savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
				savedTimeVal.put("offset", buffer.get("UTC_OFFSET").toString()  );
				
			
				// dc.append("_id",idSet);
				dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
				dc.append("savedTime",savedTimeVal);
				dc.append("fileName",buffer.get("FILENAME").toString());
				if(buffer.get("SETUP_FILE")!=null) {
					try {
						dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("SETUP_FILE").toString())  :  buffer.get("SETUP_FILE").toString());
					} catch (IllegalArgumentException e) {
						//e.printStackTrace();
						logger.info("stage1PresentProfileSetup IllegalArgumentException INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
							
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
								
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
				logger.info("stage1PresentProfileSetup update _id : "+idSet);
				mongoCollection2.updateOne(new Document().append("_id", idSet ) ,new Document().append("$set",dc),updateOptions);
				logger.info("stage1ProfileSetup insert  : "+searchProfileSetupObjectId);
			}else {
				
				idSet.put("profileInfoId", new ObjectId( mongoInfoObjectId ) );
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
							
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
								
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
				
				mongoCollection2.insertOne(dc);
				logger.info("stage1ProfileSetup insert");
				
			}
			
		}
		
		
		
		return "";
	}
	
	//TODO
	public static String stage2ProfileSetupCollectionSync( HashMap buffer , String insertMongoCollectionName , MongoDatabase mongoDatabase  ) throws SQLException {
		
		Document dc = new Document();		
		Map<String,Object> idSet = new HashMap<String,Object>();
		Map<String,Object> savedTimeVal = new HashMap<String,Object>();
		
		Bson filter = Filters.eq("seq",buffer.get("INFO_SEQ"));
		Bson sort = Sorts.descending("seq");
		MongoCollection mongoCollection = mongoDatabase.getCollection("stage2ProfileInfo");
		logger.info("stage2ProfileSetup Search seq : "+buffer.get("INFO_SEQ").toString());
		FindIterable<Document> resultObj =   mongoCollection.find(filter  ).sort(sort);
	    
		if(resultObj.first() !=null) {
			Map<String,Object> map = new HashMap<String,Object>();
			String mongoInfoObjectId =  resultObj.first().get("_id").toString();
			map.put("profileInfoId", new ObjectId(mongoInfoObjectId) );
			map.put("category", buffer.get("CATEGORY").toString()  );
			// map.put("isBackupRecord", "0".equals(buffer.get("GUBUN").toString()) ? Boolean.TRUE : Boolean.FALSE  );
			
			
			Bson searchProfileSetupObjectId = Filters.eq("_id",map);
			MongoCollection mongoCollection2 = mongoDatabase.getCollection("stage2PresentProfileSetup");
			logger.info("stage2PresentProfileSetup Search _id : "+map);
			FindIterable<Document> resultObj2  = mongoCollection2.find(searchProfileSetupObjectId);
			UpdateOptions updateOptions = new UpdateOptions().upsert(true);
		
			if( resultObj2.first() !=null ) {   // 변경된 마스터 아이디를 가져와서~
				
				// 그냥 무조건 지우고 insert
				//Bson deleteParam = Filters.eq("_id",searchProfileSetupObjectId);
				//mongoCollection2.deleteOne(deleteParam);
				idSet.put("profileInfoId",  new ObjectId( mongoInfoObjectId ) );
			    idSet.put("category", buffer.get("CATEGORY").toString()  );
				savedTimeVal.put("utc",  buffer.get("UTC_OFFSET_DATETIME").toString()  );
				savedTimeVal.put("offset", buffer.get("UTC_OFFSET").toString()  );
				
				// dc.append("_id",idSet);
				dc.append("metaDataVersion",buffer.get("METADATA_VERSION").toString());
				dc.append("savedTime",savedTimeVal);
				dc.append("fileName",buffer.get("FILENAME").toString());
				if(buffer.get("SETUP_FILE")!=null) {
					try {
						dc.append("setupFile", !"50".equals(buffer.get("CATEGORY").toString()) ? MigUtil.base64Decoding(buffer.get("SETUP_FILE").toString())  :  buffer.get("SETUP_FILE").toString());
					} catch (IllegalArgumentException e) {
						//e.printStackTrace();
						logger.info("stage1PresentProfileSetup IllegalArgumentException INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
							
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
								
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
			
				mongoCollection2.updateOne(new Document().append("_id", idSet ) ,new Document().append("$set",dc),updateOptions);
				logger.info("stage2ProfileSetup insert : "+searchProfileSetupObjectId);
			}else {
				idSet.put("profileInfoId",  new ObjectId( mongoInfoObjectId ) );
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
							
					} catch (Exception e) {
						logger.info("stage1PresentProfileSetup Exception INSERT PASS >> INFO_SEQ :"+ buffer.get("INFO_SEQ").toString() +  " CATEGORY : "+buffer.get("CATEGORY").toString() + " GUBUN : "+buffer.get("GUBUN").toString());
						//e.printStackTrace();
								
					}
				}
				if(buffer.get("RGST_DTM")!=null) {
					dc.append("createDate",buffer.get("RGST_DTM").toString());
				}
			
				mongoCollection2.insertOne(dc);
				logger.info("stage2ProfileSetup insert : "+searchProfileSetupObjectId);
			}
		}
		
		
		
		return "";
	}
	*/
}
