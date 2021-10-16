package com.block.elasticsearch.web;

import com.block.elasticsearch.pojo.PageResult;
import com.block.elasticsearch.pojo.RequestParam;
import com.block.elasticsearch.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


import java.util.List;
import java.util.Map;

/**
 * @author wangrongsong
 * @title: HotelController
 * @projectName elasticsearch-study
 * @description: TODO
 * @date 2021-10-15 16:57
 */
@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    /**
     * 根据搜索填入关键字 搜索列表
     * @param param
     * @return
     */
    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParam param){
        return hotelService.search(param);
    }
    /**
     * 根据搜索填入关键字 过滤，聚合查询，参数要与搜索列表保持一直，这样用户搜索和筛选时不会有脏数据
     * @param param
     * @return
     */
    @PostMapping("/filters")
    public Map<String, List<String>> getFilters(@RequestBody RequestParam param){
        return hotelService.filters(param);
    }


    /**
     * 自动补全搜索
     * @param prefix 用户输入的字符，做前缀匹配
     * @return
     */
    @GetMapping("/suggestion")
    public List<String> getSuggestions(@org.springframework.web.bind.annotation.RequestParam("key")String prefix){
        return hotelService.getSuggestions(prefix);
    }

}
