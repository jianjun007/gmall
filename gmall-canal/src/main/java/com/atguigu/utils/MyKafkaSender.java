package com.atguigu.utils;

import com.atguigu.constants.GmallConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author JianJun
 * @create 2021/12/4 0:40
 */
public class MyKafkaSender {

    public static KafkaProducer<String, String> kafkaProducer=null;


    public static KafkaProducer<String, String> createKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }

    public static  void send(String topic,String msg){
        if(kafkaProducer==null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,msg));


    }
}

