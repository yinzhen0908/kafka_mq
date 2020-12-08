package com.snb.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Auther:yinzhen
 * @Date:2020/11/26 15:12
 * @Description:com.snb.partitioner
 * @version: 1.0
 */
public class UserDefinePartitioner implements Partitioner {
    private AtomicInteger counter = new AtomicInteger(0);
    /**
     * 返回分区号
     * @param s
     * @param o
     * @param bytes
     * @param o1
     * @param bytes1
     * @param cluster
     * @return
     */
    @Override
    public int partition(String s,
                         Object o, byte[] bytes,
                         Object o1, byte[] bytes1,
                         Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(s);
        int numPartitions = partitions.size();

        if (bytes==null){
            int increment = counter.getAndIncrement();
            return (increment&Integer.MAX_VALUE) % numPartitions;
        }else{
            return Utils.toPositive(Utils.murmur2(bytes)) % numPartitions;
        }

    }

    //以下两个方式，是生命周期函数
    @Override
    public void close() {
        System.out.println("close");
    }

    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("configure");
    }
}
