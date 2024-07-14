package com.ym.gmall.common.base;

import com.ym.gmall.common.constant.Constant;
import com.ym.gmall.common.utils.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {
    public abstract void handle(StreamExecutionEnvironment env,
                                StreamTableEnvironment tEnv);

    public void start(int port, int parallelism, String ckAndGroupId) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        handle(env, tEnv);
    }

    // 读取 ods_db
    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table topic_db (" +
                "  `database` string, " +
                "  `table` string, " +
                "  `type` string, " +
                "  `data` map<string, string>, " +
                "  `old` map<string, string>, " +
                "  `ts` bigint, " +
                "  `pt` as proctime(), " +
                "  et as to_timestamp_ltz(ts, 0), " +
                "  watermark for et as et - interval '3' second " +
                ")" + SQLUtil.getKafkaDDLSource(groupId, Constant.TOPIC_DB));

    }

    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql(
                "create table base_dic (" +
                        " dic_code string," +  // 如果字段是原子类型,则表示这个是 rowKey, 字段随意, 字段类型随意
                        " info row<dic_name string>, " +  // 字段名和 hbase 中的列族名保持一致. 类型必须是 row. 嵌套进去的就是列
                        " primary key (dic_code) not enforced " + // 只能用 rowKey 做主键
                        ") WITH (" +
                        " 'connector' = 'hbase-2.2'," +
                        " 'table-name' = 'gmall:dim_base_dic'," +
                        " 'zookeeper.quorum' = 'localhost:2181, " +
                        " 'lookup.cache' = 'PARTIAL', " +
                        " 'lookup.async' = 'true', " +
                        " 'lookup.partial-cache.max-rows' = '20', " +
                        " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                        ")");
    }
}
