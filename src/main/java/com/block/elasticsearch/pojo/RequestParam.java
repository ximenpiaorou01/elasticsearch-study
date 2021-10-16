package com.block.elasticsearch.pojo;

import lombok.Data;

/**
 * @author wangrongsong
 * @title: RequestParam
 * @projectName elasticsearch-study
 * @description: TODO
 * @date 2021-10-15 16:54
 */
@Data
public class RequestParam {
    /**
     * 关键字
     */
    private String key;
    private Integer page;
    private Integer size;
    /**
     * 排序
     */
    private String sortBy;
    /**
     * 城市，keyword，精确查找
     */
    private String city;
    /**
     * 品牌，keyword，精确查找
     */
    private String brand;
    /**
     * 星级，keyword，精确查找
     */
    private String starName;
    /**
     * 最小价格，范围查询
     */
    private Integer minPrice;
    /**
     * 最大价格，范围查询
     */
    private Integer maxPrice;
    /**
     * 地理位置，包含latitude(维度)，longitude(经度)
     */
    private String location;
}

