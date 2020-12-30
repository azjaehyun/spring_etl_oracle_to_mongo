// servicetype
// insert 로직
1 -> profile_info -> mongo profile1Info profile2Info 초기적재  
2 -> profile_setup -> mongo profile 1 setup 초기적재
3 -> profile_setup -> mongo profile 1 setup 초기적재
4 -> profile_info -> mongo profile 2 setup 초기적재
// update 로직
5 -> profile_info -> mongo profile 1 setup 동기화  
6 -> profile_setup -> mongo profile 1 setup 동기화
7 -> profile_setup -> mongo profile 2 setup 동기화 


[ 실행시 아규 먼트 정보 ]
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
   
   
   
java -jar -Dspring.profiles.active="dev" -Ddbinfo.fromRowNum="10001" -Ddbinfo.toRowNum="20000" -Ddbinfo.infoTableFromSeq="929898699" -Ddbinfo.infoTableToSeq="929898699" -Ddbinfo.servicetype="1" -DfromUptDtm="20121220071237"  -DtoUptDtm="20121220071237" migtool-0.0.1-SNAPSHOT-spring-boot.jar

if("1".equals(servicetype)) {
	/* master table : start */
	Map<String,String> map1 = new HashMap<String, String>();
	map1.put("tableName", "Profile_Info S ");
	map1.put("whereQuery", " 1=1 ");
	map1.put("targetCollection","stage1ProfileInfo");
	map1.put("between", " AND SEQ BETWEEN");
	map1.put("fromRow",FROM_ROWNUM);
	map1.put("toRow",TO_ROWNUM);
	list.add(map1);
	
	Map<String,String> map2 = new HashMap<String, String>();
	map2.put("tableName", "Profile_Info S ");
	map2.put("whereQuery", " 1=1 ");
	map2.put("targetCollection","stage2ProfileInfo");
	map2.put("between", " AND INFO_SEQ BETWEEN");
	map2.put("fromRow",FROM_ROWNUM);
	map2.put("toRow",TO_ROWNUM);
	list.add(map2);
	/* master table end */
}

if("2".equals(servicetype)) {
	Map<String,String> map3 = new HashMap<String, String>();
	map3.put("tableName", "Profile_Setup_Data S ");
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
	map4.put("between", " AND INFO_SEQ BETWEEN");
	map4.put("fromRow",FROM_ROWNUM);
	map4.put("toRow",TO_ROWNUM);
	list.add(map4);
}

if("4".equals(servicetype)) {
	Map<String,String> map4 = new HashMap<String, String>();
	map4.put("tableName", "Profile_Setup_Data S ");
	map4.put("whereQuery", " PROFILE_LEVEL = 2  AND GUBUN = 1 ");
	map4.put("targetCollection","stage2PresentProfileSetup");
	map4.put("between", " AND INFO_SEQ BETWEEN");
	map4.put("fromRow",FROM_ROWNUM);
	map4.put("toRow",TO_ROWNUM);
	list.add(map4);
}


// update 구간

if("6".equals(servicetype)) {
	Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
	map3.put("tableName", "Profile_Setup_Data S ");
	map3.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 0 ");
	map3.put("targetCollection","stage1PresentProfileSetup");
	map3.put("between", " AND TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') BETWEEN");
	map3.put("fromRow",FROM_UPT_DTM);
	map3.put("toRow",TO_UPT_DTM);
	list.add(map3);
}

if("7".equals(servicetype)) {
	Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
	map3.put("tableName", "Profile_Setup_Data S ");
	map3.put("whereQuery", " PROFILE_LEVEL = 1  AND GUBUN = 1 AND UPT_DTM IS NULL  " );
	map3.put("targetCollection","stage1PresentProfileSetup");
	map3.put("between", " AND INFO_SEQ BETWEEN");
	map3.put("fromRow",FROM_ROWNUM);
	map3.put("toRow",TO_ROWNUM);
	list.add(map3);
}



if("8".equals(servicetype)) {
	Map<String,String> map3 = new HashMap<String, String>(); // TO_CHAR('YYYYMMDDHH24MISS')
	map3.put("tableName", "Profile_Setup_Data S ");
	map3.put("whereQuery", " PROFILE_LEVEL = 2  AND GUBUN = 1  ");
	map3.put("targetCollection","stage2PresentProfileSetup");
	map3.put("between", " AND SEQ BETWEEN");
	map3.put("fromRow",INFO_FROMSEQ);
	map3.put("toRow",INFO_TOSEQ);
	list.add(map3);
}


default properties


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
   toRowNum: 10000
   infoTableFromSeq : 1
   infoTableToSeq : 10000
   servicetype: "0"
   fromUptDtm: "20121220071237"
   toUptDtm : "20121220071237"   
   bulkUpdateYn : "N"

#spring:
#  data:
#    mongodb:
#      uri: mongodb://admin:admin@localhost:27017,localhost:27018,localhost:27019/test?authSource=admin&replicaSet=replset
#      database: test
---
logging:
  path: ./
  file: etl.log
  level:
    root: info
    sun.rmi: info
    org.mongodb: info
    org.springframework: info
    com.mongodb.migtool: debug
    com.zaxxer.hikari: off


