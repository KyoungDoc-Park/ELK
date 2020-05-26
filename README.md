# ELK project
Filebeat-> Logstash -> Elasticsearch (-> kibana)
Elasticsearch에 저장된 데이터를 query 하는 샘플

- using version : 6.4.0

## ELK configuration
src/main/resources/elk_config 아래 설정파일 위치
- filebeat.yml : 지정된 위치에서 log파일을 읽어서 logstash로 전송
- logstash.conf : filebeat에서 전송된 log파일을 받아 파싱 후 elasticsearch로 전송
- index_template  : elasticsearch에서 apigateway index생성시 해당 설정과 같이 생성
src/main/resources/script 아래 log / elastic query sample file 위치
- log_sample.log : 파일비트에서 읽어가는 로그파일 샘플
- elastic_agg_query.sample : elasticsearh aggregation query API 샘플 ( kibana dev tool 등 에서 실행가능)

## key files
com.kdpark.elasticsearch.controller.*
- AggregationController.java : RestHighLevelClient를 이용하여 Aggregation API 사용하는 방법
- ScrollController.java : RestiHighLevelClient를 이용하여 Scroll API 사용하는 방법 
aaa