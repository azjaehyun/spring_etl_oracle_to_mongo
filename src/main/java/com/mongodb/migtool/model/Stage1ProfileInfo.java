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
@Document(collation = "stage1ProfileInfo")
public class Stage1ProfileInfo {
	
	private Integer profileIndex;
	private String vin;
	private String nadID;
	private Boolean isSynced;
	private String createdTime;
	private String seq;
	     
}
