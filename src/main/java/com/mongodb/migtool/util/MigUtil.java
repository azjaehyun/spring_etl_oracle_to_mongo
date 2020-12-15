package com.mongodb.migtool.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64.Decoder;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.migtool.model.Stage1ChangedProfileSetup;
import com.mongodb.migtool.model.Stage1PresentProfilesSetup;
import com.mongodb.migtool.model.Stage1ProfileInfo;
import com.mongodb.migtool.model.Stage2ChangedProfileSetup;
import com.mongodb.migtool.model.Stage2PresentProfilesSetup;
import com.mongodb.migtool.model.Stage2ProfileInfo;

public class MigUtil {
	
	
	public static Object converTypeCasting(int columnTypeCode, Object object) throws Exception {
		Object value ;
		if( columnTypeCode == 2005 ){  // oracle clob 2005
	       value = clobToString( (Clob) object );
		}else if( columnTypeCode == 93 )  {  // oracle Timestamp
		  DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		  value = df.format( object );
	    
		}else if( columnTypeCode == 2 ) { // oracle numeric
			value = Integer.valueOf(((BigDecimal) object).intValue());
		
		}else {
			value =  object;
		}
		return value;
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
	
	public static String base64Decoding(String str) {
		Decoder decoder = Base64.getDecoder(); 
		byte[] decodedBytes = decoder.decode(str.getBytes());
		return new String(decodedBytes);
	}
	
	
}
