package com.block.elasticsearch.service.impl;


import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.block.elasticsearch.mapper.HotelMapper;
import com.block.elasticsearch.pojo.Hotel;
import com.block.elasticsearch.pojo.HotelDoc;
import com.block.elasticsearch.pojo.PageResult;
import com.block.elasticsearch.pojo.RequestParam;
import com.block.elasticsearch.service.IHotelService;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParam param) {
       try {
           //1.??????Request
           SearchRequest searchRequest = new SearchRequest("hotel");
           //2.??????DSL??????
           //2.1 ?????????????????????BooleanQuery
           buildBasicQuery(param,searchRequest);

           //2.2??????
           int page=param.getPage();
           int size=param.getSize();
           searchRequest.source().from((page-1)*size).size(size);

           //??????,??????????????????
           String location = param.getLocation();
           if(location!=null&&!location.equals("")){
               searchRequest.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(location))
                       .order(SortOrder.ASC)
                       .unit(DistanceUnit.KILOMETERS));
           }


           //????????????
           SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

           //????????????
           return handlerResponse(response);
       } catch (IOException e) {
           e.printStackTrace();
       }
       return null;
    }

    /**
     * ?????????????????????????????????????????????
     * @return
     */
    @Override
    public Map<String, List<String>> filters(RequestParam param) {
        try {
            SearchRequest searchRequest = new SearchRequest("hotel");
            //????????????????????????????????????
            buildBasicQuery(param,searchRequest);

            //?????????????????????????????????????????????????????????
            searchRequest.source().size(0);
            searchRequest.source().aggregation(AggregationBuilders
                    .terms("cityAgg")
                    .field("city")
                    .size(100));
            searchRequest.source().aggregation(AggregationBuilders
                    .terms("starAgg")
                    .field("starName")
                    .size(100));
            searchRequest.source().aggregation(AggregationBuilders
                    .terms("brandAgg")
                    .field("brand")
                    .size(100));
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = response.getAggregations();

            Map<String, List<String>> result=new HashMap<>();

            Terms cityTerm=aggregations.get("cityAgg");
            List<? extends Terms.Bucket> buckets = cityTerm.getBuckets();
            List<String> cityList = buckets.stream().map(b -> ((Terms.Bucket) b).getKeyAsString()).collect(Collectors.toList());
            result.put("city",cityList);

            Terms starTerm=aggregations.get("starAgg");
            List<String> starList = starTerm.getBuckets().stream().map(b -> ((Terms.Bucket) b).getKeyAsString()).collect(Collectors.toList());
            result.put("starName",starList);
//
            Terms brandTerm=aggregations.get("brandAgg");
            List<String> brandList = brandTerm.getBuckets().stream().map(b -> ((Terms.Bucket) b).getKeyAsString()).collect(Collectors.toList());
            result.put("brand",brandList);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * ??????????????????
     * @param prefix
     * @return
     */
    @Override
    public List<String> getSuggestions(String prefix) {
        try {
            //????????????
            SearchRequest request = new SearchRequest("hotel");
            //??????DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "hotelSuggestion",//???????????????
                    //????????????????????????????????????completion??????
                    SuggestBuilders.completionSuggestion("suggestion")//?????????????????????
                            .prefix(prefix)//???????????????
                            .skipDuplicates(true)//???????????????
                            .size(10)//??????10???
            ));
            //??????
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //????????????
            Suggest suggest = response.getSuggest();
//            Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> hotelSuggestion = suggest.getSuggestion("hotelSuggestion");
            //?????????????????????????????????
            CompletionSuggestion hotelSuggestion = suggest.getSuggestion("hotelSuggestion");
            return hotelSuggestion.getOptions().stream().map(s->s.getText().toString()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ??????id??????????????????????????????
     * @param id ??????id
     */
    @Override
    public void insertById(Long id) {
        try {
            //1.??????Request
            //1.1 ??????id????????????????????????????????????hotel??????
            Hotel hotel = getById(id);
            //?????????ES?????????HotelDoC??????
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //2.??????DSL
            IndexRequest indexRequest = new IndexRequest("hotel").id(id.toString());
            //??????JSON??????
            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

            //3.????????????
            client.index(indexRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ??????id?????????????????????doc
     * @param id ??????id
     */
    @Override
    public void deleteById(Long id) {
        try {
            //1.??????Request
            DeleteRequest deleteRequest = new DeleteRequest("hotel", id.toString());
            //2.????????????
            client.delete(deleteRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ??????????????????
     * @param param
     * @param request
     */
    private void buildBasicQuery(RequestParam param,SearchRequest request) {

        //1.????????????
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //???????????????
        if(param.getKey()==null||param.getKey().equals("")){
            //????????????
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all",param.getKey()));
        }

        //???????????????keyword??????????????????terms
        if(param.getCity()!=null&&!"".equals(param.getCity())){
            boolQuery.filter(QueryBuilders.termQuery("city",param.getCity()));
        }

        //????????????,keyword??????????????????terms
        if(param.getBrand()!=null&&!"".equals(param.getBrand())){
            boolQuery.filter(QueryBuilders.termQuery("brand",param.getBrand()));
        }

        //????????????,keyword??????????????????terms
        if(param.getStarName()!=null&&!"".equals(param.getStarName())){
            boolQuery.filter(QueryBuilders.termQuery("starName",param.getStarName()));
        }

        //?????????range
        if(param.getMinPrice()!=null&&param.getMinPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(param.getMinPrice())
                    .lte(param.getMaxPrice()));
        }

        //2.????????????,???????????????????????????
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(
                        //????????????
                        boolQuery,
                        //function score?????????
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //???????????????function score ??????
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //?????????????????????????????????
                                        QueryBuilders.termQuery("isAD",true),
                                        //????????????
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQueryBuilder);
    }

    /**
     * ????????????
     * @param response
     */
    private PageResult handlerResponse(SearchResponse response) {
        //????????????
        SearchHits searchHits = response.getHits();
        //???????????????
        TotalHits totalHits = searchHits.getTotalHits();
        long value = totalHits.value;
        PageResult pageResult = new PageResult();
        pageResult.setTotal(value);
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> res=new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            //????????????
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);

            //???????????????
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                hotelDoc.setDistance(sortValues[0]);
            }

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
            res.add(hotelDoc);
        }
        pageResult.setHotels(res);
        return pageResult;
    }
}
