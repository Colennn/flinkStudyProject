package com.ym.gmall.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ym.gmall.common.constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCDCUtil {

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 1234);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        MySqlSource<String> mysqlSource = getMysqlSource("gmall.table_process_dim");

        env.fromSource(mysqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "cdc-mysql-source")
                .setParallelism(1)
                .map(new MapFunction<String, JSONObject>() {

                    @Override
                    public JSONObject map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        return jsonObj;
                    }
                }).print();

        env.execute();

    }

    public static MySqlSource<String> getMysqlSource(String table) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList(Constant.MYSQL_DATABASE)
                .tableList(table)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .serverTimeZone("UTC")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }
}
