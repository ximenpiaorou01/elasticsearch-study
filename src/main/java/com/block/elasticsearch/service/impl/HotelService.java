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
           //1.准备Request
           SearchRequest searchRequest = new SearchRequest("hotel");
           //2.准备DSL语言
           //2.1 组合查询时构建BooleanQuery
           buildBasicQuery(param,searchRequest);

           //2.2分页
           int page=param.getPage();
           int size=param.getSize();
           searchRequest.source().from((page-1)*size).size(size);

           //排序,根据地理坐标
           String location = param.getLocation();
           if(location!=null&&!location.equals("")){
               searchRequest.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(location))
                       .order(SortOrder.ASC)
                       .unit(DistanceUnit.KILOMETERS));
           }


           //发送请求
           SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

           //解析响应
           return handlerResponse(response);
       } catch (IOException e) {
           e.printStackTrace();
       }
       return null;
    }

    /**
     * 根据城市，星级，品牌分别做聚合
     * @return
     */
    @Override
    public Map<String, List<String>> filters(RequestParam param) {
        try {
            SearchRequest searchRequest = new SearchRequest("hotel");
            //根据请求参数构建查询条件
            buildBasicQuery(param,searchRequest);

            //然后聚合，根据城市、星级和品牌分别聚合
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
     * 自动补全查询
     * @param prefix
     * @return
     */
    @Override
    public List<String> getSuggestions(String prefix) {
        try {
            //创建请求
            SearchRequest request = new SearchRequest("hotel");
            //构建DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "hotelSuggestion",//自定义名称
                    //自动补全功能的类型必须是completion类型
                    SuggestBuilders.completionSuggestion("suggestion")//查询的属性名称
                            .prefix(prefix)//输入的前缀
                            .skipDuplicates(true)//跳过重复的
                            .size(10)//获取10条
            ));
            //请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //解析请求
            Suggest suggest = response.getSuggest();
//            Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> hotelSuggestion = suggest.getSuggestion("hotelSuggestion");
            //根据上面自定义名称解析
            CompletionSuggestion hotelSuggestion = suggest.getSuggestion("hotelSuggestion");
            return hotelSuggestion.getOptions().stream().map(s->s.getText().toString()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据id新增或修改索引库中的
     * @param id 酒店id
     */
    @Override
    public void insertById(Long id) {
        try {
            //1.准备Request
            //1.1 根据id去数据库查询新增或修改的hotel信息
            Hotel hotel = getById(id);
            //封装成ES存储的HotelDoC类型
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //2.准备DSL
            IndexRequest indexRequest = new IndexRequest("hotel").id(id.toString());
            //准备JSON文档
            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

            //3.发送请求
            client.index(indexRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据id删除索引库中的doc
     * @param id 酒店id
     */
    @Override
    public void deleteById(Long id) {
        try {
            //1.准备Request
            DeleteRequest deleteRequest = new DeleteRequest("hotel", id.toString());
            //2.发送请求
            client.delete(deleteRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 构建查询参数
     * @param param
     * @param request
     */
    private void buildBasicQuery(RequestParam param,SearchRequest request) {

        //1.原始查询
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //关键字搜索
        if(param.getKey()==null||param.getKey().equals("")){
            //查询所有
            boolQuery.must(QueryBuilders.matchAllQuery());
        }else {
            boolQuery.must(QueryBuilders.matchQuery("all",param.getKey()));
        }

        //城市条件，keyword，精准匹配，terms
        if(param.getCity()!=null&&!"".equals(param.getCity())){
            boolQuery.filter(QueryBuilders.termQuery("city",param.getCity()));
        }

        //品牌条件,keyword，精准匹配，terms
        if(param.getBrand()!=null&&!"".equals(param.getBrand())){
            boolQuery.filter(QueryBuilders.termQuery("brand",param.getBrand()));
        }

        //星级条件,keyword，精准匹配，terms
        if(param.getStarName()!=null&&!"".equals(param.getStarName())){
            boolQuery.filter(QueryBuilders.termQuery("starName",param.getStarName()));
        }

        //价格，range
        if(param.getMinPrice()!=null&&param.getMinPrice()!=null){
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(param.getMinPrice())
                    .lte(param.getMaxPrice()));
        }

        //2.算分控制,用于给广告的加权重
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(
                        //原始查询
                        boolQuery,
                        //function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //其中的一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //过滤条件，满足的才加分
                                        QueryBuilders.termQuery("isAD",true),
                                        //算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

        request.source().query(functionScoreQueryBuilder);
    }

    /**
     * 处理结果
     * @param response
     */
    private PageResult handlerResponse(SearchResponse response) {
        //解析响应
        SearchHits searchHits = response.getHits();
        //获取总条数
        TotalHits totalHits = searchHits.getTotalHits();
        long value = totalHits.value;
        PageResult pageResult = new PageResult();
        pageResult.setTotal(value);
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> res=new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            //解析结果
            String source = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);

            //得到排序值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                hotelDoc.setDistance(sortValues[0]);
            }

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
            res.add(hotelDoc);
        }
        pageResult.setHotels(res);
        return pageResult;
    }
}
