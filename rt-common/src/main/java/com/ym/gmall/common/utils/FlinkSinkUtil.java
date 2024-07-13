package com.ym.gmall.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.ym.gmall.common.bean.TableProcessDim;
import com.ym.gmall.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Random;

import static com.ym.gmall.common.constant.Constant.TOPIC_DB;

public class FlinkSinkUtil {
    // 这里为什么要用 Sink<String> ？
    public static Sink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("ym-" + topic + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

}
