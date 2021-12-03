package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author JianJun
 * @create 2021/12/3 16:34
 */
@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    /**
     * 封装总数数据
     *
     * @param date
     * @return
     */
    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date) {
        //1.创建List集合用来存放最终结果数据
        ArrayList<Map> result = new ArrayList<>();

        //2.创建存放新增日活的Map
        //获取日活总数数据
        Integer dauTotal = publisherService.getDauTotal(date);

        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        //3.创建存放新增设备的Map
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value", 233);

        result.add(dauMap);
        result.add(devMap);


        return JSONObject.toJSONString(result);
    }

    /**
     * 封装分时数据
     *
     * @param id
     * @param date
     * @return
     */
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date) {


        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();


        //获取今天日活数据
        Map todayHourMap = publisherService.getDauTotalHours(date);

        //获取昨天数据
        Map yesterdayHourMap = publisherService.getDauTotalHours(yesterday);

        //创建map集合用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("yesterday", yesterdayHourMap);
        result.put("today", todayHourMap);

        return JSONObject.toJSONString(result);
    }
}
