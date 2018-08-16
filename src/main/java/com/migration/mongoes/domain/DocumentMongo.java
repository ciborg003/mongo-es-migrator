package com.migration.mongoes.domain;

import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashMap;

@Document(collection = "activitiesData")
public class DocumentMongo extends HashMap {
}
