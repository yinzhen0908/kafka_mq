package com.snb.partitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Auther:yinzhen
 * @Date:2020/11/25 11:45
 * @Description:com.snb.quickstart
 * @version: 1.0
 */
public class KafkaProducerPartitioner {
    public static void main(String[] ags) {
        //1. 创建KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //因为消息生产者，需要把消息发送给kafka的网络服务，所以需要对key和value进行序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //配置生产者的分区策略，使用自定义的分区策略
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, UserDefinePartitioner.class.getName());
        //props是设置连接kafka的连接参数
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        //发送数据给kafka的broker服务节点，发送10条消息
        for (int i = 0; i < 6; i++) {
            ProducerRecord<String, String> record =
//                    new ProducerRecord<String, String>("topic01", "key" + i, "value" + i);
                    new ProducerRecord<String, String>("topic01", "value" + i);//取消key，分区的负载均衡策略就是轮询
            producer.send(record);
        }

        producer.close();
    }
}
