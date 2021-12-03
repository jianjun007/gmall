package com.atguigu.gmallpublisher.service;

import java.util.List;
import java.util.Map;

/**
 * @author JianJun
 * @create 2021/12/3 16:28
 */
public interface PublisherService {
    public Integer getDauTotal(String date);
    public Map getDauTotalHours(String date);
}
