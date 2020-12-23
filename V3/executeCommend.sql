java -jar -Ddbinfo.fromRowNum="10001" -Ddbinfo.toRowNum="20000" -Ddbinfo.servicetype="1" -DuptDtm="20121220071237" migtool-0.0.1-SNAPSHOT-spring-boot.jar



dbinfo:
   oracleip: 10.10.19.121
   oracleport: 1521
   oracleid: was_iss
   oraclepw: "!1234was_iss"
   oracledatabasename: gen2db
   mongouri:  mongodb://ccs_toRM:!1234ccs_toRM@10.11.52.246:27017,10.11.52.247:27017,10.11.52.248:27017/ApptxPersonalization
   mongodatabasename: ApptxPersonalization
   mongodatabaseargs: authSource=admin&replicaSet=testReplSet&readPreference=primary&appname=MongoDB%20Compass&ssl=false
   fromRowNum: 1
   toRowNum: 1000
   servicetype: insert
   uptDtm: "20121220071237"

#spring:
#  data:
#    mongodb:
#      uri: mongodb://admin:admin@localhost:27017,localhost:27018,localhost:27019/test?authSource=admin&replicaSet=replset
#      database: test
---
logging:
  level:
    root: info
    sun.rmi: info
    org.mongodb: info
    org.springframework: info
    com.mongodb.migtool: debug
    com.zaxxer.hikari: off


		if("1".equals(servicetype)) {
			/* master table : start */
			Map<String,String> map1 = new HashMap<String, String>();
			map1.put("tableName", "Profile_Info");
			map1.put("whereQuery", "SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +" ");
			map1.put("targetCollection","stage1ProfileInfo");
			list.add(map1);
			
			Map<String,String> map2 = new HashMap<String, String>();
			map2.put("tableName", "Profile_Info");
			map2.put("whereQuery", " SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +" ");
			map2.put("targetCollection","stage2ProfileInfo");
			list.add(map2);
			/* master table end */
		}
		
		if("2".equals(servicetype)) {
			Map<String,String> map3 = new HashMap<String, String>();
			map3.put("tableName", "Profile_Setup_Data");
			map3.put("whereQuery", " INFO_SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +" AND PROFILE_LEVEL = 1  AND TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') <= "+UPT_DTM + "  ");
			map3.put("targetCollection","stage1PresentProfileSetup");
			list.add(map3);
		}

		if("3".equals(servicetype)) {
			Map<String,String> map4 = new HashMap<String, String>();
			map4.put("tableName", "Profile_Setup_Data");
			map4.put("whereQuery", " INFO_SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM +"  AND PROFILE_LEVEL = 2  AND GUBUN = 1 ");
			map4.put("targetCollection","stage2PresentProfileSetup");
			list.add(map4);
		}
		
		/*sync*/
		
		if("4".equals(servicetype)) {
			/* master table : start */
			Map<String,String> map1 = new HashMap<String, String>();
			map1.put("tableName", "Profile_Info");
			map1.put("whereQuery", "SEQ > "+ TO_ROWNUM +" ");
			map1.put("targetCollection","stage1ProfileInfo");
			list.add(map1);
			
		}
		
		if("5".equals(servicetype)) {
			Map<String,String> map2 = new HashMap<String, String>();
			map2.put("tableName", "Profile_Info");
			map2.put("whereQuery", "SEQ > "+ TO_ROWNUM +" ");
			map2.put("targetCollection","stage2ProfileInfo");
			list.add(map2);
			/* master table end */
		}

		
		
		if("6".equals(servicetype)) {
			Map<String,String> map3 = new HashMap<String, String>();
			map3.put("tableName", "Profile_Setup_Data");
			map3.put("whereQuery", " ( INFO_SEQ > "+ TO_ROWNUM +" AND PROFILE_LEVEL = 1 ) or ( TO_CHAR(UPT_DTM,'yyyyMMddHH24miss') > "+ UPT_DTM +" )  ");
			map3.put("targetCollection","stage1PresentProfileSetup");
			list.add(map3);
		}
		
		if("7".equals(servicetype)) {
			Map<String,String> map4 = new HashMap<String, String>();
			map4.put("tableName", "Profile_Setup_Data");
			map4.put("whereQuery", " INFO_SEQ > "+ TO_ROWNUM +"  AND PROFILE_LEVEL = 2  AND GUBUN = 1 ");
			map4.put("targetCollection","stage2PresentProfileSetup");
			list.add(map4);
		}
		
		
		if("10".equals(servicetype)) {
			Map<String,String> map4 = new HashMap<String, String>();
			map4.put("tableName", "Profile_Setup_Data");
			map4.put("whereQuery", "  PROFILE_LEVEL = 1 AND GUBUN = 1 AND INFO_SEQ BETWEEN "+ FROM_ROWNUM +" AND "+ TO_ROWNUM + " ");
			map4.put("targetCollection","stage1PresentProfileSetup");
			list.add(map4);
		}
		