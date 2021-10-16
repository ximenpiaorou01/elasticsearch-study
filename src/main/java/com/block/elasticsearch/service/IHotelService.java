package com.block.elasticsearch.service;


import com.baomidou.mybatisplus.extension.service.IService;
import com.block.elasticsearch.pojo.Hotel;
import com.block.elasticsearch.pojo.PageResult;
import com.block.elasticsearch.pojo.RequestParam;

import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {
    PageResult search(RequestParam param);

    Map<String, List<String>> filters(RequestParam param);

    List<String> getSuggestions(String prefix);
}
