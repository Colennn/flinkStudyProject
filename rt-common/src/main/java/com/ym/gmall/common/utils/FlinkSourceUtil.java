package com.ym.gmall.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.ym.gmall.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class FlinkSourceUtil {

    public static void main(String[] args) {
        System.out.println(getKafkaSource("kafkaSourceTest", Constant.TOPIC_DB));
    }

    public static KafkaSource<String> getKafkaSource(String groupId, String topicId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId) // 定义: Group ID是一组消费者的标识符，这些消费者共同消费一个或多个topic。Group ID用于区分不同的消费组，即使它们可能消费相同的topic。
                .setTopics(topicId)
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新的offset开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 教程里面写了一个匿名内部类没啥用，多此一举
                .build();
    }

}
