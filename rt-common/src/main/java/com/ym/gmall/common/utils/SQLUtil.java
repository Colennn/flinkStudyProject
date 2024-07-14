package com.ym.gmall.common.utils;

import com.ym.gmall.common.constant.Constant;

public class SQLUtil {

    public static String getKafkaDDLSource(String groupId, String topicId) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topicId + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," + // 当 json 解析失败的时候,忽略这条数据
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaDDLSink(String topicId) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topicId + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'format' = 'json' " +
                ")";
    }

    public static String getUpsertKafkaDDL(String topicId) {
        return "with(" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '" + topicId + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.ignore-parse-errors' = 'true'," +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
