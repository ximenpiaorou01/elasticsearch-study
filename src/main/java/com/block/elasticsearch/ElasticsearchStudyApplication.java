package com.block.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@MapperScan("com.block.elasticsearch.mapper")
@SpringBootApplication
public class ElasticsearchStudyApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchStudyApplication.class, args);
    }

    @Bean
    public RestHighLevelClient client(){
        return new RestHighLevelClient(RestClient.builder(
           new HttpHost("192.168.57.131",9200)
        ));
    }

}
