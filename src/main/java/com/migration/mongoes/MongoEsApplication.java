package com.migration.mongoes;

import com.migration.mongoes.helper.ElasticsearchTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
public class MongoEsApplication implements ApplicationRunner {

	@Autowired
	private MigrationService migrationService;


	public static void main(String[] args) {
		SpringApplication.run(MongoEsApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		migrationService.start();
	}
}
