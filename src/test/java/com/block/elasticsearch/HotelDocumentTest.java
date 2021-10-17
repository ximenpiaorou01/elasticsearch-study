package com.block.elasticsearch;


import com.alibaba.fastjson.JSON;
import com.block.elasticsearch.pojo.Hotel;
import com.block.elasticsearch.pojo.HotelDoc;
import com.block.elasticsearch.service.IHotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
class HotelDocumentTest {

    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    /**
     * 创建文档
     * @throws IOException
     */
    @Test
    void testAddDocument() throws IOException {

        //根据id去数据库查询Hotel
        Hotel hotel = hotelService.getById(61083l);
        //需要转换为文档类型，把hotel中的longitude和latitude转换为索引库中的location字段
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1.查询索引库hotel数据
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        //2.把数据转出json
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3.发送请求
        client.index(request,RequestOptions.DEFAULT);
    }

    /**
     * 根据id查询文档
     * @throws IOException
     */
    @Test
    void testGetDocumentById() throws IOException {
        // 1.准备Request      // GET /hotel/_doc/{id}
        GetRequest request = new GetRequest("hotel", "61083");
        // 2.发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3.解析响应结果
        String source = response.getSourceAsString();
        HotelDoc doc = JSON.parseObject(source, HotelDoc.class);
        System.out.println(doc);

    }


    /**
     * 根据id删除文档
     * @throws IOException
     */
    @Test
    void testDeleteDocumentById() throws IOException {
        // 1.准备Request      // DELETE /hotel/_doc/{id}
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 更新文档字段消息
     * @throws IOException
     */
    @Test
    void testUpdateById() throws IOException {
        // 1.准备Request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备参数
        request.doc(
                "price","952",
                "starName","四砖"
        );
        // 3.发送请求
        client.update(request,RequestOptions.DEFAULT);
    }

    /**
     * 批量增加文档到索引库
     * @throws IOException
     */
    @Test
    void testBulkRequest() throws IOException {
        List<Hotel> list = hotelService.list();
        BulkRequest bulkRequest = new BulkRequest();
        for (Hotel hotel : list) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotel.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        client.bulk(bulkRequest,RequestOptions.DEFAULT);
    }

    @BeforeEach
    void setUp() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.57.131:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close();
    }




}
