package com.ym.gmall.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.ym.gmall.common.bean.TableProcessDim;
import com.ym.gmall.common.bean.TableProcessDwd;
import com.ym.gmall.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
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

    public static Sink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> dataWithConfig,
                                                                    KafkaSinkContext kafkaSinkContext,
                                                                    Long aLong) {
                        String topic = dataWithConfig.f1.getSinkTable();
                        JSONObject data = dataWithConfig.f0;
                        data.remove("op_type"); // 忘了这个字段是什么意思？
                        return new ProducerRecord<>(topic, data.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("ym-" + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    public static DorisSink<String> getDorisSink(String table, String labelPrefix) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(table)
                        .setUsername("root")
                        .setPassword("000000")
                        .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        .setLabelPrefix(labelPrefix)  // stream-load 导入数据时 label 的前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(1024 * 1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔  上述三个批次的限制条件是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }

}
