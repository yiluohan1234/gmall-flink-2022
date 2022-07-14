package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

// 抽象方法可以提到接口中
public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;
}
