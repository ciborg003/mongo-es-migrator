package com.migration.mongoes.configuration;

import com.migration.mongoes.helper.ElasticsearchTemplate;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URL;

@Configuration
public class ElasticsearchConfiguration {

    @Value("${org.elasticsearch.http.uri}")
    private String esUri;

    @Bean
    public ElasticsearchTemplate elasticsearchTemplate() throws MalformedURLException {

        URL uri = new URL(esUri);

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(uri.getHost(),uri.getPort(), uri.getProtocol())
                )
        );

        return new ElasticsearchTemplate(client);
    }

}
