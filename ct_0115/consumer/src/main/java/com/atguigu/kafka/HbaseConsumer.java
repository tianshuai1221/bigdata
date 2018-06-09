package com.atguigu.kafka;

import com.atguigu.dao.HbaseDAO;
import com.atguigu.utils.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

public class HbaseConsumer {

    public static void main(String[] args) throws IOException, ParseException {

        //获取kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(PropertyUtil.properties);

        kafkaConsumer.subscribe(Collections.singletonList(PropertyUtil.properties.getProperty("kafka.topic")));

        //创建HbaseDAO对象（作用：写入数据）
       HbaseDAO hbaseDAO = new HbaseDAO();
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(300);
            System.out.println(records.isEmpty());
            for (ConsumerRecord<String, String> record : records) {
                //14314302040,19460860743,2019-05-08 23:41:05,0439
                String ori = record.value();
                System.out.println(ori);
                hbaseDAO.put(ori);
            }
        }

    }
}
