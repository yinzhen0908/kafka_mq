package com.snb.offsets;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

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
public class KafkaConsumerOffset04 {
    public static void main(String[] ags) {
        //1. 创建消费者KafkaConsumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //因为消息消费者，需要从kafka的网络服务获取消息，所以需要对key和value进行反序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //在kafka中，采用订阅模式时，消费者一定要属于某一个消费组，以组的形式去管理消费者，所以，要配置消费者的组信息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g5");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //offset偏移量自动提交设置为false，即关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        //一旦，启动成功后，这两个设置的消费者，后续的读取都一样了，因为，消费者消费完成后，会自动向系统提交偏移量

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //2. 订阅相关的topics,参数可以是若干个topics，也可以是正则表达式
        //这个模式下，我们的消费者是处在一种订阅的形式下的，在这种模式下，我们必须设置消费者所属的消费者组
        //它的特性：当当前组的消费者宕机时，kafka会自动将这个消费者所分配的分区分给其他的消费者
        consumer.subscribe(Arrays.asList("topic01"));

        //3. 遍历消息队列
        while (true){
            //获取记录，设置隔多长时间抓取数据，设置隔一秒获取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {//从队列中取到了数据
                Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
                //1 这个map，用于记录，此次poll操作所获取的，分区的，消费元数据信息
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                while (recordIterator.hasNext()) {
                    //获取一个消费消息
                    ConsumerRecord<String, String> record = recordIterator.next();
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();
                    //2 记录消费者的分区的偏移量元数据，一定要记住，在提交的时候，偏移量信息offset+1，否则每次都能读取到上次的最后一条数据，因为，这里提交的
                    //offset，应该是下一次读取的偏移量位置
                    offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset+1));
                    //3 提交消费者偏移量，异步提交
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            System.out.println("offsets: "+offsets+" exception: "+exception);
                        }
                    });
                    System.out.println(topic+"\t"+partition+","+offset+"\t"+key+" "+value+" "+timestamp);

                }
            }
        }


    }
}
