package com.block.elasticsearch.config;

import com.block.elasticsearch.constants.MqConstants;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author wangrongsong
 * @title: MqConfig
 * @projectName elasticsearch-study
 * @description: TODO
 * @date 2021-10-16 18:37
 */
@Configuration
public class MqConfig {

    /**
     * 注入一个Topic类型的交换机
     * @return
     */
    @Bean
    public TopicExchange topicExchange(){
        return new TopicExchange(MqConstants.HOTEL_EXCHANGE,true,false);
    }

    /**
     * 注入一个新增或修改的队列
     * @return
     */
    @Bean
    public Queue insertQueue(){
        return new Queue(MqConstants.HOTEL_INSERT_QUEUE,true);
    }

    /**
     * 注入一个删除的队列
     * @return
     */
    @Bean
    public Queue deleteQueue(){
        return new Queue(MqConstants.HOTEL_DELETE_QUEUE);
    }

    /**
     * 把插入队列绑定到交换机
     */
    @Bean
    public Binding insertQueueBinding(){
        return BindingBuilder
                .bind(insertQueue())
                .to(topicExchange())
                .with(MqConstants.HOTEL_INSERT_KEY);
    }

    /**
     * 把删除队列绑定到交换机
     */
    @Bean
    public Binding deleteQueueBinding(){
        return BindingBuilder
                .bind(deleteQueue())
                .to(topicExchange())
                .with(MqConstants.HOTEL_DELETE_KEY);
    }
}
