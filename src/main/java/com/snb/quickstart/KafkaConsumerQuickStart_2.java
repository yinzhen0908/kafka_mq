package com.snb.quickstart;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @Auther:yinzhen
 * @Date:2020/11/25 11:45
 * @Description:com.snb.quickstart
 * @version: 1.0
 */
public class KafkaConsumerQuickStart_2 {
    public static void main(String[] ags) {
        //1. 创建KafkaConsumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //因为消息消费者，需要从kafka的网络服务获取消息，所以需要对key和value进行反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //2. 订阅相关的topics，assign 手动指定一个或者多个消费分区，此时就失去组管理特性，消费者之间就没有关系，这里就是指定消费topic为topic01的第0个分区
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic01", 0));
        consumer.assign(partitions);
        //consumer.seekToBeginning(partitions);//指定消费分区的位置，从指定分区的第0位开始消费
        consumer.seek(new TopicPartition("topic01",0), 5);//从指定偏移量位置开始消费

        //3. 遍历消息队列
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            if (!records.isEmpty()) {//从队列中取到了数据
                Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
                while (recordIterator.hasNext()) {
                    //获取一个消费消息
                    ConsumerRecord<String, String> record = recordIterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();
                    System.out.println(topic+"\t"+partition+","+offset+"\t"+key+" "+value+" "+timestamp);
                }
            }
        }

    }
}
