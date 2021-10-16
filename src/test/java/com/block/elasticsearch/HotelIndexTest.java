package com.block.elasticsearch;

import com.block.elasticsearch.constants.HotelIndexConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.IOException;
import static com.block.elasticsearch.constants.HotelIndexConstants.MAPPING_TEMPLATE;


@SpringBootTest
class HotelIndexTest {

    private RestHighLevelClient client;

    /**
     * 表示一开始就完成成员变量舒初始化
     */
    @BeforeEach
    void setUp(){
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.57.131:9200")
//                ,HttpHost.create("http://192.168.57.131:9200")
//                ,HttpHost.create("http://192.168.57.131:9200")
                ));
    }

    /**
     * 销毁
     */
    @AfterEach
    void tearDown() {
        try {
            this.client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testInit(){
        System.out.println(client);
    }

    /**
     * 创建索引库
     * @throws IOException
     */
    @Test
    void createHotelIndex() throws IOException {
        //1.插件Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.准备请求的参数:DSL语句
        request.source(MAPPING_TEMPLATE,XContentType.JSON);
        //3.发送请求
        client.indices().create(request,RequestOptions.DEFAULT);

    }

    /**
     * 删除索引
     */
    @Test
    void deleteHotelIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        client.indices().delete(request,RequestOptions.DEFAULT);
    }

    /**
     * 判断索引是否存在
     */
    @Test
    void isExists() throws IOException {
        GetIndexRequest req = new GetIndexRequest("hotel");
        boolean exists = client.indices().exists(req, RequestOptions.DEFAULT);
        System.out.println(exists?"索引库存在！":"索引库不存在");
    }

}
