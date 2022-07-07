package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@RestController = @Controller+@ResponseBody
@RestController //表示返回普通对象而不是页面
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/test")
    public String test() {
        System.out.println("success");
        return "success";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam(value = "age",defaultValue = "18") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @RequestMapping("/applog")
    public String getLog(@RequestParam("param") String jsonStr) {

        //落盘
        log.info(jsonStr);

        //写入 Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";

    }
}
