package com.snb.quickstart;

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
public class KafkaProducerQuickStart {
    public static void main(String[] ags) {
        //1. 创建生产者 KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        //因为消息生产者，需要把消息发送给kafka的网络服务，所以需要对key和value进行序列化
        //kafka已经内建了一些序列化的规则，这里用的是StringSerializer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //两个泛型，分别表示，这个生产者未来发送给kafka的key和value的类型
        //props是设置连接kafka的连接参数
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        //发送数据给kafka的broker服务节点，发送消息
        for (int i = 0; i < 10; i++) {
            //构建需要发送的记录
            //1.可以发送给指定的分区 2.可以设置key，则对key使用hash算法，来发送给对应的分区 3.不设置key，则采用轮询的算法，发送给分区
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("topic01","key" + i, "value" + i);//hash
//                    new ProducerRecord<String, String>("topic01", "value" + i);//取消key，分区的负载均衡策略就是轮询
            //发送消息给服务器
            producer.send(record);
        }

        producer.close();
    }
}
