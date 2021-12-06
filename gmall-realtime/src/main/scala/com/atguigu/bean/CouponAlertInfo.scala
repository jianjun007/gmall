package com.atguigu.bean

/**
 * @author JianJun
 * @create 2021/12/6 8:47
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
