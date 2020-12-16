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
@Document(collation = "testInsert")
public class TestInsert {
	@Id
	private ObjectId _id;

}
