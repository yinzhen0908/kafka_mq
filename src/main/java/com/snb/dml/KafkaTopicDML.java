package com.snb.dml;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @Auther:yinzhen
 * @Date:2020/11/24 19:19
 * @Description:com.snb.dml
 * @version: 1.0
 */
//必须将CentOSA、B、C这三个主机的ip和主机名映射配置在当前代码所运行的操作系统的/etc/hosts文件中，否则连接不上消息服务器
public class KafkaTopicDML {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建KafkaAdminClient，props是kafka的连结参数
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"CentOSA:9092,CentOSB:9092,CentOSC:9092");
        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

        //创建topic信息
        //kafka提供的所有的client，所有指令都是异步的，所以，看不到创建形式的
        CreateTopicsResult createTopicsResult1 = adminClient.createTopics(Arrays.asList(new NewTopic("topic01", 3, (short) 3)));
        createTopicsResult1.all().get();//增加这一行就变成同步创建

        /*CreateTopicsResult createTopicsResult2 = adminClient.createTopics(Arrays.asList(new NewTopic("topic02", 3, (short) 3)));
        createTopicsResult2.all().get();//同步创建*/

        /*CreateTopicsResult createTopicsResult3 = adminClient.createTopics(Arrays.asList(new NewTopic("topic03", 3, (short) 3)));
        createTopicsResult3.all().get();*/

        //删除topic
        /*DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("topic01"));
        deleteTopicsResult.all().get();//同步删除*/

        //查看topic列表
        ListTopicsResult topicsResult2 = adminClient.listTopics();
        Set<String> names2 = topicsResult2.names().get();
        for (String name : names2) {
            System.out.println(name);
        }

        //查看topic详细信息
        /*DescribeTopicsResult dtr = adminClient.describeTopics(Arrays.asList("topic01"));
        Map<String, TopicDescription> topicDescriptionMap = dtr.all().get();
        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }*/

        adminClient.close();
    }
}
