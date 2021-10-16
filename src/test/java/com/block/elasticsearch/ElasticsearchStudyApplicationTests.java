package com.block.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.block.elasticsearch.pojo.HotelDoc;
import com.block.elasticsearch.service.IHotelService;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
class ElasticsearchStudyApplicationTests {

    private RestHighLevelClient client;
    @Autowired
    private IHotelService hotelService;
    /**
     * 加载完成就初始化
     */
    @BeforeEach
    void setUp() {
        this.client=new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.57.131",9200)));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    /**
     * 查询指定索引库的所有文档
     */
    @Test
    void testMatchAll() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //查询所有
        searchReq.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);

        //解析响应
        handlerResponse(response);

    }

    /**
     * 查询指定索引库的
     * all
     */
    @Test
    void testAll() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //查询复合字段
        searchReq.source().query(QueryBuilders.matchQuery("all","如家"));
        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }


    /**
     * 查询指定索引库的
     * bolean
     */
    @Test
    void testBolean() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //封装boolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //添加term,
        boolQuery.must(QueryBuilders.termQuery("city","上海"));
        //添加range
        //加个小于等于250元
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        searchReq.source().query(boolQuery);

        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }


    /**
     * 查询指定索引库的
     * sorted和分页
     */
    @Test
    void testSortedAndPage() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //1.查询
        searchReq.source().query(QueryBuilders.matchAllQuery());
        //2.排序sort
        searchReq.source().sort("price", SortOrder.ASC);
        //分页
        searchReq.source().from(0).size(5);

        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }



    /**
     * 查询指定索引库的
     * 高亮显示
     */
    @Test
    void testHight() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //1.查询
        searchReq.source().query(QueryBuilders.matchQuery("all","如家"));

        //给name属性添加高亮显示，并且不与字段匹配，因为查的是all[name,brand,business]
        searchReq.source().highlighter(new HighlightBuilder()
                .field("name")
                .requireFieldMatch(false));

        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }

    /**
     * 聚合-Aggregation，以bulket为单位
     */
    @Test
    void testAggregation() throws IOException {
        //1.准备Reuqest
        SearchRequest searchRequest = new SearchRequest("hotel");
        //准备DSL
        //2.1设置size为0表示把文档数据清掉，不需要查出来
        searchRequest.source().size(0);
        searchRequest.source().aggregation(AggregationBuilders
                .terms("brandAgg")//自定义的聚合名称
                .field("brand")//需要聚合的属性
                .size(20));//条数

        //请求数据
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析结果
        Aggregations aggregations = response.getAggregations();
        //根据聚合名称获取聚合结果
        Terms terms = aggregations.get("brandAgg");
        //获取buckets
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println(bucket.getKey());
        });


    }

    /**
     * 聚合查询
     */
    @Test
    void testAggregationList(){
        Map<String, List<String>> filters = hotelService.filters(null);
        System.out.println(filters);
    }

    /**
     * 自动补全查询
     */
    @Test
    void testSuggest() throws IOException {
        //准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source().suggest(new SuggestBuilder()
                .addSuggestion(
                   "suggestions",//自定义名称
                        //自动补全的类型必须是completion
                        SuggestBuilders.completionSuggestion("suggestion")//属性名称
                                .prefix("h")//搜索时输入的字符
                                .skipDuplicates(true)//跳过重复的
                                .size(10)//取10条
                ));
        //3.请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        suggestions.getOptions().stream().map(s->s.getText().toString()).collect(Collectors.toList()).forEach(System.out::println);

    }





    /**
     * 处理结果
     * @param response
     */
    private void handlerResponse(SearchResponse response) {
        //解析响应
        SearchHits searchHits = response.getHits();
        //获取总条数
        TotalHits totalHits = searchHits.getTotalHits();
        long value = totalHits.value;
        System.out.println("获取总条数:" + value);

        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            //解析结果
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);

            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields)){
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField!=null){
                    String string = highlightField.getFragments()[0].string();
                    //覆盖hotelDoc里面name值
                    hotelDoc.setName(string);
                }
            }
            System.out.println(hotelDoc);
        }
    }


}
