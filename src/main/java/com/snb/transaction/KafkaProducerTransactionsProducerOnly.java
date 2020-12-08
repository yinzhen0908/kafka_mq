package com.snb.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * @Auther:yinzhen
 * @Date:2020/11/25 11:45
 * @Description:com.snb.quickstart
 * @version: 1.0
 */
public class KafkaProducerTransactionsProducerOnly {
    public static void main(String[] ags) {
        KafkaProducer<String, String> producer = buildKafkaProducer();

        //1 初始化事务
        producer.initTransactions();
        try {
            //2 开启事务
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                //模拟异常，让事务终止
                /*if (i==8){
                    int j = 10/0;
                }*/
                ProducerRecord<String, String> record =
                        //new ProducerRecord<String, String>("topic01","transaction" + i, "error data" + i);//hash
                        new ProducerRecord<String, String>("topic01","transaction" + i, "right data!!" + i);//hash
                producer.send(record);
                producer.flush();//确保每条数据都发送到kafka
            }
            //3 事务提交
            producer.commitTransaction();
        }catch (Exception e){
            System.err.println("出现错误："+e.getMessage());
            //4 终止事务
            producer.abortTransaction();
        }finally {
            producer.close();
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
}
