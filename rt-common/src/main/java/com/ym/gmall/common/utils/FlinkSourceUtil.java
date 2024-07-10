package com.ym.gmall.common.utils;

import com.ym.gmall.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String groupId, String topicId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topicId)
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新的offset开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 教程里面写了一个匿名内部类没啥用，多此一举
                .build();
    }
}
