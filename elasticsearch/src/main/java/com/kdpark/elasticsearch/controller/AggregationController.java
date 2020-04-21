package com.kdpark.elasticsearch.controller;

import java.io.IOException;
import java.util.List;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AggregationController {
	
	@Autowired RestHighLevelClient client;
	
	final Long retvPerd = 10000L; // histogram 검색 단위 시간(10s = 10000ms)

	
	/**
	 * 응답평균시간 리스트
	 * 현재시간 ~ 현재시간+10  의 시간사이에서 10초간격으로 집계 추출  
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/getApiAverageReplyTime")
	public Aggregations getApiAverageReplyTime() throws IOException {
		
		SearchRequest searchRequest = new SearchRequest("apigateway-*");
		searchRequest.types("logs");		
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.size(0);	
	
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		DateTime endDateTime = new DateTime().withZone(DateTimeZone.forID("UTC"));  //검색 종료시간
		endDateTime = endDateTime.withSecondOfMinute(0).withMillisOfSecond(0);
			
		//temp end 
		DateTime startDateTime = endDateTime.minusMinutes(10).minusSeconds(1).withZone(DateTimeZone.forID("UTC"));  //검색시작시간  (시작시간의 10분전)

		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));

log.error("ee");
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		//범위 쿼리  설정
		searchSourceBuilder.query(QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery("eventTime").gt(startDateTime.toString(fmt)).lt(endDateTime.toString(fmt))));
		
		//aggregation serviceId
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("byService").field("serviceId").size(20);
		//date histogram
		DateHistogramAggregationBuilder dhAggregation = new DateHistogramAggregationBuilder("avgRespTime").field("eventTime").interval(retvPerd); 
		//aggregation avg, max, min
		AvgAggregationBuilder avgAggAggregation = AggregationBuilders.avg("avgTime").field("responseTime");
		MaxAggregationBuilder maxAggAggregation = AggregationBuilders.max("maxTime").field("responseTime");
		MinAggregationBuilder minAggAggregation = AggregationBuilders.min("minTime").field("responseTime");
		
		dhAggregation.subAggregation(avgAggAggregation);
		dhAggregation.subAggregation(maxAggAggregation);
		dhAggregation.subAggregation(minAggAggregation);
		
		aggregation.subAggregation(dhAggregation);	
		
		searchSourceBuilder.aggregation(aggregation);	
		
		searchRequest.source(searchSourceBuilder);
		
		//ElasticSearch 쿼리 실행
		SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
				
		int totalShards = response.getTotalShards();
		int successfulShards = response.getSuccessfulShards();
		int failedShards = response.getFailedShards();
		long hitCnt = response.getHits().totalHits;		
		log.info("totalShard : "+totalShards+", successShard : "+successfulShards+" failedShard : "+failedShards+", hitCnt : "+hitCnt);
		
		Aggregations aggregations = response.getAggregations();		
		Terms byServiceAggregation = aggregations.get("byService");		
		log.info("docCountError : "+byServiceAggregation.getDocCountError(), " getSumOfOtherDocCounts : "+	byServiceAggregation.getSumOfOtherDocCounts());
				
		List<? extends Bucket> elasticBucket = byServiceAggregation.getBuckets();


		//add properties
		//add properties
		//add properties
		//add properties//add properties
		//add properties
		//add properties//add properties




		//get specific data
		for(Bucket serviceBucket : elasticBucket) {
			log.info("key : "+serviceBucket.getKeyAsString());
			
			ParsedDateHistogram avgRespTimeAgg = serviceBucket.getAggregations().get("avgRespTime");		
			List<? extends org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket> avgRespTimeBucket = avgRespTimeAgg.getBuckets();
									
			int count = 0;
			for (org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket timeBucket : avgRespTimeBucket) {
				
				if(avgRespTimeBucket.size() ==(count+1) ) {
					log.info("avgRespTimeBucket.size() : "+avgRespTimeBucket.size()+"  count: "+(count+1));
					continue;
				}else {
					Aggregations timeAgg = timeBucket.getAggregations();
					Avg avgTime = timeAgg.get("avgTime");
					Max maxTime = timeAgg.get("maxTime");
					Min minTime = timeAgg.get("minTime");
					count++;
					
				}									    
			}		
		}		
		return aggregations;				
	}
	
	

	/**
	 * API 종류별 카운트
	 * 현재시간 ~ 현재시간+10  의 시간사이에서 10초간격으로 집계 추출  
	 * @return 
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/getApiCnt")
	public Aggregations insertApiCnt() throws IOException {
		
		SearchRequest searchRequest = new SearchRequest("apigateway-*");
		searchRequest.types("logs");		
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.size(0);	
		
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		DateTime endDateTime = new DateTime().withZone(DateTimeZone.forID("UTC"));  //검색 종료시간
		endDateTime = endDateTime.withSecondOfMinute(0).withMillisOfSecond(0);
		
		DateTime startDateTime = endDateTime.minusMinutes(10).minusSeconds(1).withZone(DateTimeZone.forID("UTC"));  //검색시작시간  (시작시간의 10분전)
			
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		
		//범위 쿼리 
		searchSourceBuilder.query(QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery("eventTime").gt(startDateTime.toString(fmt)).lt(endDateTime.toString(fmt))));
		
		//aggregation serviceId
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("byService").field("serviceId").size(20);
		//date histogram
		DateHistogramAggregationBuilder dhAggregation = new DateHistogramAggregationBuilder("avgRespTime").field("eventTime").interval(retvPerd); 
		//aggregation avg, max, min
		TermsAggregationBuilder apiUrlAggregation = AggregationBuilders.terms("apiCount").field("requestUrl").size(100);
		dhAggregation.subAggregation(apiUrlAggregation);
			
		aggregation.subAggregation(dhAggregation);
		
		searchSourceBuilder.aggregation(aggregation);	
				
		searchRequest.source(searchSourceBuilder);
		
		//ElasticSearch 쿼리 실행
		SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
				
		int totalShards = response.getTotalShards();
		int successfulShards = response.getSuccessfulShards();
		int failedShards = response.getFailedShards();
		long hitCnt = response.getHits().totalHits;		
		log.info("totalShard : "+totalShards+", successShard : "+successfulShards+" failedShard : "+failedShards+", hitCnt : "+hitCnt);
		
		
		Aggregations aggregations = response.getAggregations();		
		Terms byServiceAggregation = aggregations.get("byService");		
		log.info("docCountError : "+byServiceAggregation.getDocCountError()+" getSumOfOtherDocCounts: "+byServiceAggregation.getSumOfOtherDocCounts());
				
		List<? extends Bucket> elasticBucket = byServiceAggregation.getBuckets();
		
		for(Bucket serviceBucket : elasticBucket) {
			log.info("key : "+serviceBucket.getKeyAsString());
			
			ParsedDateHistogram avgRespTimeAgg = serviceBucket.getAggregations().get("avgRespTime");		
			List<? extends org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket> avgRespTimeBucket = avgRespTimeAgg.getBuckets();
			
			int timeBucketCount = 0; 
			for (org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket timeBucket : avgRespTimeBucket) {
				
				String key = timeBucket.getKeyAsString();
				Terms apiResult = timeBucket.getAggregations().get("apiCount");
				//insert with Local Time			    
			    DateTime stDt = new DateTime(timeBucket.getKeyAsString());
			    DateTime trans = stDt.withZone(DateTimeZone.forID("Asia/Seoul"));			
			    timeBucketCount++;
			}
			
		}		
				
		return aggregations;
	}
	
	
	/**
	 * 응답코드 통계
	 * 현재시간 ~ 현재시간+10  의 시간사이에서 10초간격으로 집계 추출  
	 * @return 
	 * @return
	 * @throws IOException
	 */
	@GetMapping("/getReplyCodeStat")
	public Aggregations getReplyCodeStat() throws IOException {
		
		SearchRequest searchRequest = new SearchRequest("apigateway-*");
		searchRequest.types("logs");		
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.size(0);	
	
		DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		DateTime endDateTime = new DateTime().withZone(DateTimeZone.forID("UTC"));  //검색 종료시간
		endDateTime = endDateTime.withSecondOfMinute(0).withMillisOfSecond(0);
		DateTime startDateTime = endDateTime.minusMinutes(10).minusSeconds(1).withZone(DateTimeZone.forID("UTC"));  //검색시작시간  (시작시간의 10분전)
			
		log.info("startdt: "+startDateTime.toString(fmt));
		log.info("enddt  : "+endDateTime.toString(fmt));
		
		//범위 쿼리 
		searchSourceBuilder.query(QueryBuilders.boolQuery()
				.must(QueryBuilders.rangeQuery("eventTime").gt(startDateTime.toString(fmt)).lt(endDateTime.toString(fmt))));
		
		//aggregation serviceId
		TermsAggregationBuilder aggregation = AggregationBuilders.terms("byService").field("serviceId").size(20);
		//date histogram
		DateHistogramAggregationBuilder dhAggregation = new DateHistogramAggregationBuilder("avgRespTime").field("eventTime").interval(retvPerd); 
		//aggregation avg, max, min
		TermsAggregationBuilder apiUrlAggregation = AggregationBuilders.terms("responseCodeCnt").field("responseCode").size(20);
		dhAggregation.subAggregation(apiUrlAggregation);
			
		aggregation.subAggregation(dhAggregation);
		
		searchSourceBuilder.aggregation(aggregation);	
		
		searchRequest.source(searchSourceBuilder);
		
		//ElasticSearch 쿼리 실행
		SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
		
						
		int totalShards = response.getTotalShards();
		int successfulShards = response.getSuccessfulShards();
		int failedShards = response.getFailedShards();
		long hitCnt = response.getHits().totalHits;		
		log.info("totalShard : "+totalShards+", successShard : "+successfulShards+" failedShard : "+failedShards+", hitCnt : "+hitCnt);
		
		
		Aggregations aggregations = response.getAggregations();		
		Terms byServiceAggregation = aggregations.get("byService");		
		log.info("docCountError : "+byServiceAggregation.getDocCountError()+" getSumOfOtherDocCounts: "+byServiceAggregation.getSumOfOtherDocCounts());
				
		List<? extends Bucket> elasticBucket = byServiceAggregation.getBuckets();
			
		for(Bucket serviceBucket : elasticBucket) {
			log.info("key : "+serviceBucket.getKeyAsString());
			
			ParsedDateHistogram avgRespTimeAgg = serviceBucket.getAggregations().get("avgRespTime");		
			List<? extends org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket> avgRespTimeBucket = avgRespTimeAgg.getBuckets();
					
			int timeBucketCount = 0; 
			for (org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket timeBucket : avgRespTimeBucket) {
				
				String key = timeBucket.getKeyAsString();
				Terms rpyCdCnt = timeBucket.getAggregations().get("responseCodeCnt");
				//insert with Local Time			    
			    DateTime stDt = new DateTime(timeBucket.getKeyAsString());
			    DateTime trans = stDt.withZone(DateTimeZone.forID("Asia/Seoul"));	   	    
			}
		
		}		
	
		return aggregations;
		
	}
	
	
	
	
	
}
