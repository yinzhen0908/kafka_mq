package com.snb.transaction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

//开启类的并行运行模式：点击这个类，选择edit configurations，勾选allow parallel run
//启动消费者，启动成功后，有一个消费者协调器ConsumerCoordinator，负责将topic中的分区分发给当前的消费者
//同一组内的消费者，对topic的消费是均分分区的，再启动一个消费者实例，kafka就会重新分配每个消费者消费的分区
//不同组的消费者，他们之间没有均分的概念

/**
 * @Auther:yinzhen
 * @Date:2020/11/25 11:45
 * @Description:com.snb.quickstart
 * @version: 1.0
 */
public class KafkaConsumerTransactionReadCommitted {
    public static void main(String[] ags) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        //设置消费者的消费事务的隔离级别为read_committed（读已提交），即此时消费者无法消费已经终止的事务
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("topic01"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
                while (recordIterator.hasNext()) {
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
