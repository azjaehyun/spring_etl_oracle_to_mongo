package com.mongodb.migtool.model;

import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
@ConstructorBinding
@AllArgsConstructor
@Document(collation = "stage2ProfileInfo")
public class Stage2ProfileInfo {
	private Integer profileIndex;
	private String profileName;
	private String profileID;
	private String phoneNum;
	private String vin;
	private String nadID;
	private String createdTime;
	private String seq;
	
}
