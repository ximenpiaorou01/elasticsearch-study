package com.block.elasticsearch.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author wangrongsong
 * @title: PageResult
 * @projectName elasticsearch-study
 * @description: TODO
 * @date 2021-10-15 16:55
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResult {
    private Long total;
    private List<HotelDoc> hotels;
}
