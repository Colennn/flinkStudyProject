package com.ym.gmall.ods;

import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ym.gmall.common.base.BaseApp;
import com.ym.gmall.common.utils.FlinkCDCUtil;
import com.ym.gmall.common.utils.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ym.gmall.common.constant.Constant.MYSQL_TABLELIST;
import static com.ym.gmall.common.constant.Constant.TOPIC_DB;

/**
 * 用于模拟从 MySQL 读取业务数据
 */
public class OdsApp extends BaseApp {
    public static void main(String[] args) {
        new OdsApp().start(1000, 1, TOPIC_DB, null);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 读取 mysql 所有表
        // 2. 生产数据到 kafka 主题 topic-db
        MySqlSource<String> mysqlSource = FlinkCDCUtil.getMysqlSource(MYSQL_TABLELIST);
        DataStreamSource<String> mysqlCDC =
                env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc");
        mysqlCDC.print();
        mysqlCDC.map(JSON::toJSONString).sinkTo(FlinkSinkUtil.getKafkaSink(TOPIC_DB));
    }
}
