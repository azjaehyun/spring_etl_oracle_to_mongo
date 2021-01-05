// -Ddbinfo.servicetype 아규먼트 

-Ddbinfo.servicetype 숫자에 따라 아래와 같이 적재가 실행된다. 
// insert 로직
-Ddbinfo.servicetype="1"  -> profile_info -> mongo profile1Info profile2Info 초기적재  실행시 아규먼트 -Ddbinfo.bulkUpdateYn="N"
-Ddbinfo.servicetype="2"  -> profile_setup -> mongo profile 1 setup 초기적재  where 조건 >> PROFILE_LEVEL = 1  AND GUBUN = 0 
-Ddbinfo.servicetype="3"  -> profile_setup -> mongo profile 1 setup 초기적재  where 조건 >> PROFILE_LEVEL = 1  AND GUBUN = 1
-Ddbinfo.servicetype="4"  -> profile_info -> mongo profile 2 setup 초기적재   where 조건 >> PROFILE_LEVEL = 2 
-Ddbinfo.servicetype="10" -> profile_setup -> mongo profile 1 setup 초기적재  where 조건 >> PROFILE_LEVEL = 1
-Ddbinfo.servicetype="11" -> profile_setup -> mongo profile 1 setup 초기적재  where 조건 >> PROFILE_LEVEL = 1

// update 로직
-Ddbinfo.servicetype="1" -> profile_info -> mongo profile1Info profile2Info 증분적재  실행시 아규먼트 -Ddbinfo.bulkUpdateYn="Y"
-Ddbinfo.servicetype="5" -> profile_info -> mongo profile 1 setup 동기화  where 조건 >> PROFILE_LEVEL = 1  AND GUBUN = 0 
-Ddbinfo.servicetype="6" -> profile_setup -> mongo profile 1 setup 동히과  where 조건 >> PROFILE_LEVEL = 1  AND GUBUN = 1
-Ddbinfo.servicetype="7" -> profile_setup -> mongo profile 2 setup 동기화  where 조건 >> PROFILE_LEVEL = 2 


[ 실행시 아규 먼트 정보 ]
default application.yml 정보 
dbinfo:
   oracleip: 10.10.84.150
   oracleport: 1521
   oracleid: adm_vit
   oraclepw: "vit_adm123!"
   oracledatabasename: vitdb
   mongouri:  mongodb://pcsadm:pcsadm%2100@10.11.63.112:27017,10.11.63.113:27017,10.11.63.114:27017/
   mongodatabasename: ApptxPersonalization
   mongodatabaseargs: replicaSet=PCSReplSet&authSource=admin
   fromRowNum: 1
   toRowNum: 1000
   infoTableFromSeq : 929898699
   infoTableToSeq : 929898699
   servicetype: "0"
   fromUptDtm: "20121220071237"
   toUptDtm : "20121220071237"   
   
주의 !! -Dspring.profiles.active="dev" << 테스트 서버일때만 넣고 리얼서버는 아규먼트 빼거나 -Dspring.profiles.active="default"
   
   
로그파라미터  -Dlogging.file="logname"
< dev >   
< 초기 적재 >
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="1" -Ddbinfo.bulkUpdateYn="N"  -Dlogging.file="etl2.log" migtool-0.0.1-SNAPSHOT-spring-boot.jar // 초기 적재
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="2" -Ddbinfo.toUptDtm="20201230165100"  -Dlogging.file="etl2.log" migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="3"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="4"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="10"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="11"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert

< 증분 적재 >
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="1" -Ddbinfo.bulkUpdateYn="Y"  migtool-0.0.1-SNAPSHOT-spring-boot.jar // 증분

>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.toUptDtm="20201231165100" -Ddbinfo.servicetype="6" -Ddbinfo.bulkUpdateYn="Y" migtool-0.0.1-SNAPSHOT-spring-boot.jar 
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="7" -Ddbinfo.bulkUpdateYn="Y" migtool-0.0.1-SNAPSHOT-spring-boot.jar 
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.infoTableFromSeq="10000" -Ddbinfo.infoTableToSeq="929898699" -Ddbinfo.servicetype="8" -Ddbinfo.bulkUpdateYn="Y" migtool-0.0.1-SNAPSHOT-spring-boot.jar 

< 초기 적재 >
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="1" -Ddbinfo.bulkUpdateYn="N"  migtool-0.0.1-SNAPSHOT-spring-boot.jar // 초기 적재
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="2" -Ddbinfo.toUptDtm="20201230165100"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="3"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="4"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="10"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="11"  migtool-0.0.1-SNAPSHOT-spring-boot.jar  // insert

< 증분 적재 >
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="1" -Ddbinfo.bulkUpdateYn="Y"  migtool-0.0.1-SNAPSHOT-spring-boot.jar // 증분

>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromUptDtm="20201230165100" -Ddbinfo.toUptDtm="20201231165100" -Ddbinfo.servicetype="6" -Ddbinfo.bulkUpdateYn="Y" migtool-0.0.1-SNAPSHOT-spring-boot.jar 
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="1" -Ddbinfo.toRowNum="100000" -Ddbinfo.servicetype="7" -Ddbinfo.bulkUpdateYn="Y" migtool-0.0.1-SNAPSHOT-spring-boot.jar 
>>  java -jar -Dspring.profiles.active="dev" -Ddbinfo.infoTableFromSeq="10000" -Ddbinfo.infoTableToSeq="929898699" -Ddbinfo.servicetype="8" -Ddbinfo.bulkUpdateYn="Y" migtool-0.0.1-SNAPSHOT-spring-boot.jar 


//  초기 적재  
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
	map2.put("between", " AND SEQ BETWEEN ");
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


// update  구간
if("6".equals(servicetype)) {
	Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
	map3.put("tableName", "Profile_Setup_Data S ");
	map3.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 0 AND TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') >= "+TO_UPT_DTM + "  ");
	// SEQ > 20000
	map3.put("targetCollection","stage1PresentProfileSetup");
	map3.put("between", " AND INFO_SEQ BETWEEN");
	map3.put("fromRow", FROM_ROWNUM);
	map3.put("toRow", TO_ROWNUM);
	list.add(map3);
}

if("7".equals(servicetype)) {
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


// 초기 적재
// delta -> stage1ChangedProfileSetup 초기 적재
if("10".equals(servicetype)) {
	Map<String,String> map5 = new HashMap<String, String>();
	map5.put("tableName", "Profile_Setup_Delta");
	map5.put("whereQuery", "  PROFILE_LEVEL = 1 ");
	map5.put("targetCollection","stage1ChangedProfileSetup");
	map5.put("between", " AND INFO_SEQ BETWEEN ");
	map5.put("fromRow",FROM_ROWNUM);
	map5.put("toRow",TO_ROWNUM);
	list.add(map5);
	
}

// 초기 적재
// delta -> stage2ChangedProfileSetup 초기 적재
if("11".equals(servicetype)) {
	Map<String,String> map6 = new HashMap<String, String>();
	map6.put("tableName", "Profile_Setup_Delta");
	map6.put("whereQuery", "  PROFILE_LEVEL = 2 ");
	map6.put("targetCollection","stage2ChangedProfileSetup");
	map6.put("between", " AND INFO_SEQ BETWEEN ");
	map6.put("fromRow",FROM_ROWNUM);
	map6.put("toRow",TO_ROWNUM);
	list.add(map6);
}


