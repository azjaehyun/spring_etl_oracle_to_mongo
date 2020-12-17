package com.mongodb.migtool.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.xml.bind.DatatypeConverter;

public class MigUtil {
	
	
	public static Object converTypeCasting(int columnTypeCode, Object object) throws Exception {
		Object value ;
		if( columnTypeCode == 2005 ){  // oracle clob 2005
	       value = clobToString( (Clob) object );
		}else if( columnTypeCode == 93 )  {  // oracle Timestamp
		  DateFormat df = new SimpleDateFormat("yyyyMMddHH:mm:ss");
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
		// Decoder decoder = Base64.getDecoder();
		// byte[] decodedBytes = decoder.decode(str.getBytes());
		// byte[] decodedBytes = str.getBytes();
		byte[] decodedBytes = DatatypeConverter.parseBase64Binary(str);		
		return new String(decodedBytes,StandardCharsets.UTF_8);
	}
	
	
}
