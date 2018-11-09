package com.kdpark.elasticsearch.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;


import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ScrollController {
	
	
	@Autowired RestHighLevelClient client;

	
	@GetMapping("/insertApiAvgRpyTimeWithScroll")
	public boolean insertApiAvgRpyTimeWithScroll() throws IOException {
	
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10L));
		
		SearchRequest searchRequest = new SearchRequest("apigateway-*");
		searchRequest.types("logs");	
		searchRequest.scroll(scroll);		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.size(10000);
		
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		
		DateTime endDateTime = new DateTime().withZone(DateTimeZone.forID("UTC"));  //검색 종료시간
		endDateTime = endDateTime.withSecondOfMinute(0).withMillisOfSecond(0);
		//temp end 
		DateTime startDateTime = endDateTime.minusDays(1).withZone(DateTimeZone.forID("UTC"));  //검색시작시간  (시작시간의 10분전)
			
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		
		searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("eventTime").gt(startDateTime.toString(fmt)).lt(endDateTime.toString(fmt))));	
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT); 
		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		log.info("count : "+searchHits.length);
		
		HashMap<String,Object> statApiAvgRpyTimeResult = new HashMap<>();
		HashMap<String,Object> statApiUseCascntResult = new HashMap<>();
		HashMap<String,Object> statRpycdResult = new HashMap<>();
		
		//first scroll result
		for(SearchHit  hit : searchHits) {		 			
	    	Map<String, Object> sourceAsMap = hit.getSourceAsMap();	    	
	    	statApiAvgRpyTimeResult = calculateApiAvgRpyTimeResult(statApiAvgRpyTimeResult, sourceAsMap);
	    	statApiUseCascntResult = calculateApiUseCascntResult(statApiUseCascntResult,sourceAsMap);
	    	statRpycdResult = calculateRpycdResult(statRpycdResult,sourceAsMap);
	    }
		
		//query next scroll
		while (searchHits != null && searchHits.length > 0) { 
			SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
			scrollRequest.scroll(scroll);
			searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
			scrollId = searchResponse.getScrollId();
			searchHits = searchResponse.getHits().getHits();
			log.info("count : "+searchHits.length);
			
			for(SearchHit  hit : searchHits) {		 			
		    	Map<String, Object> sourceAsMap = hit.getSourceAsMap();	    	
		    	statApiAvgRpyTimeResult = calculateApiAvgRpyTimeResult(statApiAvgRpyTimeResult, sourceAsMap);
		    	statApiUseCascntResult = calculateApiUseCascntResult(statApiUseCascntResult,sourceAsMap);
		    	statRpycdResult = calculateRpycdResult(statRpycdResult,sourceAsMap);
		    }
		}			
				
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); 
		clearScrollRequest.addScrollId(scrollId);
		ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
		boolean succeeded = clearScrollResponse.isSucceeded();
				
		
		
		return succeeded;
				
	}
	
	

	// calculate max, min value 
	private HashMap<String,Object> calculateApiAvgRpyTimeResult(HashMap<String,Object> services,  Map<String,Object> sourceAsMap ){
		
		HashMap<String,Object> tempServices = services;
		
	 	int hitResponseTime= Integer.parseInt((String)sourceAsMap.get("responseTime"));	    	
    	String hitServiceId= (String)sourceAsMap.get("serviceId");
    	
    	if(services.containsKey(hitServiceId)){
    		HashMap<String,Object> tempMap = (HashMap<String, Object>) tempServices.get(hitServiceId);
    		tempMap.put("count", new Integer((int) tempMap.get("count")+1));
    		tempMap.put("total", new Long( (long) tempMap.get("total")+hitResponseTime));
    		
    		if((int)tempMap.get("max") < hitResponseTime) {
    			tempMap.put("max", hitResponseTime);
    		}
    		if((int)tempMap.get("min") > hitResponseTime) {
    			tempMap.put("min", hitResponseTime);
    		}    		
    		
    		tempServices.put(hitServiceId, tempMap);
    	}else {
    		HashMap<String,Object> info = new HashMap<>();
    		info.put("max",0);
    		info.put("min",0);
    		info.put("avg",0d);
    		info.put("total",0L);
    		info.put("count",0);    		
    		services.put(hitServiceId, info);
    		HashMap<String,Object> tempMap = (HashMap<String, Object>) tempServices.get(hitServiceId);
    		tempMap.put("count", new Integer((int) tempMap.get("count")+1));
    		tempMap.put("total", new Long( (long) tempMap.get("total")+hitResponseTime));
    		tempMap.put("max", hitResponseTime);
    		tempMap.put("min", hitResponseTime);
    		tempServices.put(hitServiceId, tempMap);
    	}
    	
		return tempServices;
	}
	
	private HashMap<String, Object> calculateApiUseCascntResult(HashMap<String, Object> services, Map<String, Object> sourceAsMap) {

		HashMap<String,Object> tempServices = services;
		
		String hitServiceId= (String)sourceAsMap.get("serviceId");
    	String hitRequestUrl= (String)sourceAsMap.get("requestUrl");
    	
    	if(services.containsKey(hitServiceId)){
    		HashMap<String,Integer> tempMap = (HashMap<String, Integer>) tempServices.get(hitServiceId);
    		if(tempMap.containsKey(hitRequestUrl)) {
    			tempMap.put(hitRequestUrl, tempMap.get(hitRequestUrl)+1);
    			tempServices.put(hitServiceId,tempMap);
    		}else {
    			tempMap.put(hitRequestUrl,1);
    			tempServices.put(hitServiceId,tempMap);
    		}
    		
    	}else {
    		HashMap<String,Object> info = new HashMap<>();    		
    		info.put(hitRequestUrl,1);    		
    		services.put(hitServiceId, info);
    	}

		return tempServices;
	}

	
	private HashMap<String, Object> calculateRpycdResult(HashMap<String, Object> services, Map<String, Object> sourceAsMap) {

		HashMap<String,Object> tempServices = services;
		
		String hitServiceId= (String)sourceAsMap.get("serviceId");
    	String hitResponseCode= (String)sourceAsMap.get("responseCode");
		
    	if(services.containsKey(hitServiceId)){
    		HashMap<String,Integer> tempMap = (HashMap<String, Integer>) tempServices.get(hitServiceId);
    		if(tempMap.containsKey(hitResponseCode)) {
    			tempMap.put(hitResponseCode, tempMap.get(hitResponseCode)+1);
    			tempServices.put(hitServiceId,tempMap);
    		}else {
    			tempMap.put(hitResponseCode,1);
    			tempServices.put(hitServiceId,tempMap);
    		}
    		
    	}else {
    		HashMap<String,Object> info = new HashMap<>();    		
    		info.put(hitResponseCode,1);    		
    		services.put(hitServiceId, info);
    	}

		return tempServices;
	}
}
