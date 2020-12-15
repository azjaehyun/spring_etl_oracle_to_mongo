package com.mongodb.migtool.model;

import org.bson.types.ObjectId;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.data.annotation.Id;
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
@Document(collation = "stage2PresentProfilesSetup")
public class Stage2PresentProfilesSetup {
	@Id
	private ProfileInfo _id;
	private String metaDataVersion;
	private SaveTimeInfo savedTime;
	private String fileName;
	private String setupFile;
	private String createdDate;
	
	@Getter
	@Setter
	@Builder
	public static class ProfileInfo{
		private ObjectId profileInfoId;
		private String category;
	}
	
	@Getter
	@Setter
	@Builder
	public static class SaveTimeInfo{
		private String utc;
		private String offset;
	}
}
