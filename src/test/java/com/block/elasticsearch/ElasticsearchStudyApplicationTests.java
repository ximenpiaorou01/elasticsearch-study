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
     * ????????????????????????
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
     * ????????????????????????????????????
     */
    @Test
    void testMatchAll() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //????????????
        searchReq.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);

        //????????????
        handlerResponse(response);

    }

    /**
     * ????????????????????????
     * all
     */
    @Test
    void testAll() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //??????????????????
        searchReq.source().query(QueryBuilders.matchQuery("all","??????"));
        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }


    /**
     * ????????????????????????
     * bolean
     */
    @Test
    void testBolean() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //??????boolQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //??????term,
        boolQuery.must(QueryBuilders.termQuery("city","??????"));
        //??????range
        //??????????????????250???
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        searchReq.source().query(boolQuery);

        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }


    /**
     * ????????????????????????
     * sorted?????????
     */
    @Test
    void testSortedAndPage() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //1.??????
        searchReq.source().query(QueryBuilders.matchAllQuery());
        //2.??????sort
        searchReq.source().sort("price", SortOrder.ASC);
        //??????
        searchReq.source().from(0).size(5);

        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }



    /**
     * ????????????????????????
     * ????????????
     */
    @Test
    void testHight() throws IOException {
        SearchRequest searchReq = new SearchRequest("hotel");
        //1.??????
        searchReq.source().query(QueryBuilders.matchQuery("all","??????"));

        //???name?????????????????????????????????????????????????????????????????????all[name,brand,business]
        searchReq.source().highlighter(new HighlightBuilder()
                .field("name")
                .requireFieldMatch(false));

        SearchResponse response = client.search(searchReq, RequestOptions.DEFAULT);
        handlerResponse(response);

    }

    /**
     * ??????-Aggregation??????bulket?????????
     */
    @Test
    void testAggregation() throws IOException {
        //1.??????Reuqest
        SearchRequest searchRequest = new SearchRequest("hotel");
        //??????DSL
        //2.1??????size???0????????????????????????????????????????????????
        searchRequest.source().size(0);
        searchRequest.source().aggregation(AggregationBuilders
                .terms("brandAgg")//????????????????????????
                .field("brand")//?????????????????????
                .size(20));//??????

        //????????????
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        //????????????
        Aggregations aggregations = response.getAggregations();
        //????????????????????????????????????
        Terms terms = aggregations.get("brandAgg");
        //??????buckets
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(bucket->{
            System.out.println(bucket.getKey());
        });


    }

    /**
     * ????????????
     */
    @Test
    void testAggregationList(){
        Map<String, List<String>> filters = hotelService.filters(null);
        System.out.println(filters);
    }

    /**
     * ??????????????????
     */
    @Test
    void testSuggest() throws IOException {
        //??????Request
        SearchRequest request = new SearchRequest("hotel");
        //2.??????DSL
        request.source().suggest(new SuggestBuilder()
                .addSuggestion(
                   "suggestions",//???????????????
                        //??????????????????????????????completion
                        SuggestBuilders.completionSuggestion("suggestion")//????????????
                                .prefix("h")//????????????????????????
                                .skipDuplicates(true)//???????????????
                                .size(10)//???10???
                ));
        //3.??????
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //????????????
        Suggest suggest = response.getSuggest();
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        suggestions.getOptions().stream().map(s->s.getText().toString()).collect(Collectors.toList()).forEach(System.out::println);

    }





    /**
     * ????????????
     * @param response
     */
    private void handlerResponse(SearchResponse response) {
        //????????????
        SearchHits searchHits = response.getHits();
        //???????????????
        TotalHits totalHits = searchHits.getTotalHits();
        long value = totalHits.value;
        System.out.println("???????????????:" + value);

        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            //????????????
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);

            //??????????????????
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields)){
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField!=null){
                    String string = highlightField.getFragments()[0].string();
                    //??????hotelDoc??????name???
                    hotelDoc.setName(string);
                }
            }
            System.out.println(hotelDoc);
        }
    }


}
