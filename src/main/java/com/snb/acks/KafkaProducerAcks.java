package com.snb.acks;

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
public class KafkaProducerAcks {
    public static void main(String[] ags) {
        //1. 创建生产者 KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOSA:9092,CentOSB:9092,CentOSC:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //设置kafka的acks以及retries
        props.put(ProducerConfig.ACKS_CONFIG, "all");//all也可以用-1代替，都是代表leader将等待全套同步副本确认后，再记录数据
        props.put(ProducerConfig.RETRIES_CONFIG, 3);//设置重发次数，不包含第一次发送，如果发送尝试3次失败，则系统放弃发送
        //为了让请求超时，将检测超时的时间设置为1ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("topic01","ack", "test ack");
        //发送消息给服务器
        producer.send(record);
        producer.flush();//防止kafka缓冲

        producer.close();
    }
}
