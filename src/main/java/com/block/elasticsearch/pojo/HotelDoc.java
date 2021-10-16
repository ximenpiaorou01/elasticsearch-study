package com.block.elasticsearch.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
public class HotelDoc {
    private Long id;
    /**
     * 酒店名称
     */
    private String name;
    /**
     * 地址
     */
    private String address;
    /**
     * 价格
     */
    private Integer price;
    /**
     * 评分
     */
    private Integer score;
    /**
     * 品牌
     */
    private String brand;
    /**
     * 城市
     */
    private String city;
    /**
     * 星级
     */
    private String starName;
    /**
     * 商圈
     */
    private String business;
    /**
     * 位置：包含经纬度 latitude，longitu
     */
    private String location;
    /**
     * 酒店图片
     */
    private String pic;
    /**
     * 距离，离自己位置的距离
     */
    private Object distance;
    /**
     * 是否广告
     */
    private Boolean isAD;
    /**
     * 用户自动补全的内容
     * 比如品牌brand和商圈business
     */
    private List<String> suggestion;


    public HotelDoc(Hotel hotel) {
        this.id = hotel.getId();
        this.name = hotel.getName();
        this.address = hotel.getAddress();
        this.price = hotel.getPrice();
        this.score = hotel.getScore();
        this.brand = hotel.getBrand();
        this.city = hotel.getCity();
        this.starName = hotel.getStarName();
        this.business = hotel.getBusiness();
        this.location = hotel.getLatitude() + ", " + hotel.getLongitude();
        this.pic = hotel.getPic();
        if(this.business.contains("/")){
            String[] bus = this.business.split("/");
            this.suggestion=new ArrayList<>();
            suggestion.add(this.brand);
            Collections.addAll(this.suggestion,bus);
        }else {
            this.suggestion= Arrays.asList(this.brand,this.business);

        }
    }
}
