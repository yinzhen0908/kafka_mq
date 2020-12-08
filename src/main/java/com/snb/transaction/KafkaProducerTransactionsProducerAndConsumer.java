package com.snb.transaction;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * @Auther:yinzhen
 * @Date:2020/11/25 11:45
 * @Description:com.snb.quickstart
 * @version: 1.0
 */
public class KafkaProducerTransactionsProducerAndConsumer {
    public static void main(String[] ags) {
        KafkaProducer<String, String> producer = buildKafkaProducer();
        KafkaConsumer<String, String> consumer = buildKafkaConsumer("g1");

        //1 初始化事务
        producer.initTransactions();

        consumer.subscribe(Arrays.asList("topic01"));
        while (true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
                //开启事务控制
                producer.beginTransaction();
                try {
                    //迭代数据，进行业务处理
                    while (recordIterator.hasNext()) {
                        ConsumerRecord<String, String> record = recordIterator.next();
                        //存储元数据
                        offsets.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1));
                        //业务代码，将从topic01中获取的数据，进行包装后，写入topic02中
                        ProducerRecord<String, String> pRecord = new ProducerRecord<String, String>("topic02", record.key(), record.value() + "mashibing edu online");
                        producer.send(pRecord);
                    }

                    //提交事务
                    producer.sendOffsetsToTransaction(offsets,"g1");//提交消费者的偏移量
                    producer.commitTransaction();//提交生产者事务
                } catch (Exception e) {
                    System.err.println("错误了~"+e.getMessage());
                    //终止事务
                    producer.abortTransaction();
                }

            }
        }

    }
    public static KafkaProducer<String,String> buildKafkaProducer(){
        //1. 创建生产者 KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //必须配置事务id，必须唯一
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id"+ UUID.randomUUID().toString());
        //配置kafka批处理大小，就是累积达到这个大小的消息后，才将这批消息发送给kafka
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        //等待时间(单位是毫秒，默认是0，不等待），即无论是不是累积到批处理大小的消息，
        // 达到这个等待时间后，生产者都将消息发送给kafka
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        //配置kafka的重试机制和幂等机制
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20000);

        return new KafkaProducer<String, String>(props);
    }
    public static KafkaConsumer<String,String> buildKafkaConsumer(String groupId){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //设置消费者的消费事务的隔离级别为read_committed（读已提交），即此时消费者无法消费已经终止的事务
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        //因为要做生产者消费者事务，必须关闭消费者端的offset自动提交，因为，必须等生产者的业务逻辑完全处理完成之后
        //才允许做事务的提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        return new KafkaConsumer<String, String>(props);
    }

}
