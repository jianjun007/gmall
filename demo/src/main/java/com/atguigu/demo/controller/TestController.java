package com.atguigu.demo.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author JianJun
 * @create 2021/12/3 12:24
 */
@RestController
public class TestController {

    @RequestMapping("test01")
    public String sayHi() {
        System.out.println("Hi");
        return "Success!";
    }

    @RequestMapping("test02")
    public String sayHi(
            @RequestParam("name") String name,
            @RequestParam("age") Integer age) {
        System.out.println("Hi~我是" + age + "岁的" + name);
        return "name:" + name + ",age:" + age;

    }

}
