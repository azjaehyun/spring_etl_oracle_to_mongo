package com.mongodb.migtool.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Base64.Decoder;

public class MigUtil {
	
	
	public static Object converTypeCasting(int columnTypeCode, Object object) throws Exception {
		Object value ;
		if(object !=null) {
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
		}else {
			return "";
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
	
	public static String base64Decoding(String str) throws Exception , IllegalArgumentException {
		Decoder decoder = Base64.getDecoder();
		byte[] decodedBytes = decoder.decode(str.getBytes("EUC-KR"));
		//byte[] decodedBytes = str.getBytes();
		//byte[] decodedBytes = DatatypeConverter.parseBase64Binary(str);		
		return new String(decodedBytes);
	}
	
	
}
