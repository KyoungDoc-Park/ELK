package com.kdpark.elasticsearch.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.elasticsearch.client.RestHighLevelClient;


@Configuration
public class ElasticConfig {
	 
	 @Value("#{'${elasticsearch.cluster-nodes}'.split(',')}")	 
	 private List<String> hosts;
	
	 @Value("${elasticsearch.cluster-port}")
	 private int port;
	 
	 private final String elasticScheme = "http";
	//asdss
	 @Bean
     public RestHighLevelClient getRestClient() {
	
		@SuppressWarnings("unused")
		RestHighLevelClient restHighLevelClient = null; 
		  List<HttpHost> hostList = new ArrayList<>();
	        for(String host : hosts) {
	            hostList.add(new HttpHost(host, port, elasticScheme));
	        }

	        RestClientBuilder builder = RestClient.builder(hostList.toArray(new HttpHost[hostList.size()]));
	        return new RestHighLevelClient(builder);
     }

}
