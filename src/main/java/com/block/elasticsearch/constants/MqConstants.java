package com.block.elasticsearch.constants;

/**
 * @author wangrongsong
 * @title: MqConstants
 * @projectName elasticsearch-study
 * @description: TODO
 * @date 2021-10-16 18:30
 */
public class MqConstants {
    /**
     * 交换机
     */
    public final static String HOTEL_EXCHANGE="hotel.topic";

    /**
     * 监听新增或修改的队列（ES的新增和修改是一个接口，id存在是修改，id不存在是新增）
     */
    public final static String HOTEL_INSERT_QUEUE="hotel.insert.queue";

    /**
     * 监听删除的队列
     */
    public final static String HOTEL_DELETE_QUEUE="hotel.delete.queue";

    /**
     * 新增或修改的routingKey
     */
    public static final String HOTEL_INSERT_KEY="hotel.insert";

    /**
     * 删除的routingKey
     */
    public static final String HOTEL_DELETE_KEY="hotel.delete";
}
