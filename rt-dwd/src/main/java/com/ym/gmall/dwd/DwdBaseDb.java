package com.ym.gmall.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ym.gmall.common.base.BaseApp;
import com.ym.gmall.common.bean.TableProcessDwd;
import com.ym.gmall.common.constant.Constant;
import com.ym.gmall.common.utils.FlinkSinkUtil;
import com.ym.gmall.common.utils.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import static com.ym.gmall.common.utils.RedisUtil.getKey;

@Slf4j
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(2002, 1, "dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 消费 kafka 数据，对数据进行清洗
        SingleOutputStreamOperator<JSONObject> etl = etl(stream);
        // 2. 通过 flinkCDC 读取配置数据？这里不是在 dim 层做了吗？
        SingleOutputStreamOperator<TableProcessDwd> configStream = readTableProcess(env);
        // 3. 数据流关联配置流，先把配置流做成广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream = connect(etl, configStream);
        // 4. 删除不需要的数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream =
                deleteNotNeedColumns(dataWithConfigStream);
        // 5. 数据下沉到 kafka
        writeToKafka(resultStream);

    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultStream) {
        resultStream.sinkTo(FlinkSinkUtil.getKafkaSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> dataWithConfigStream) {
        return dataWithConfigStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, Tuple2<JSONObject, TableProcessDwd>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcessDwd> map(Tuple2<JSONObject, TableProcessDwd> dataWithConfig) throws Exception {
                        JSONObject data = dataWithConfig.f0;
                        List<String> columns = new ArrayList<>(Arrays.asList(dataWithConfig.f1.getSinkColumns().split(",")));

                        data.keySet().removeIf(key -> !columns.contains(key));
                        return dataWithConfig;
                    }
                });
    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connect(
            SingleOutputStreamOperator<JSONObject> etl, SingleOutputStreamOperator<TableProcessDwd> configStream) {
        // 1. 把配置流做成广播流
        // key: 表名:type   user_info:ALL
        // value: TableProcess
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDwd>("table_process_dwd", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = configStream.broadcast(mapStateDescriptor);

        return etl.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {

            private HashMap<String, TableProcessDwd> map;
            @Override
            public void open(Configuration parameters) throws Exception {
                // open 中没有办法访问状态!!!
                map = new HashMap<>();
                // 1. 去 mysql 中查询 table_process 表所有数据
                java.sql.Connection mysqlConn = JdbcUtil.getMysqlConnection();
                List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mysqlConn,
                        "select * from gmall2023_config.table_process_dwd",
                        TableProcessDwd.class,
                        true
                );

                // 先从 redis 里面获取数据
                for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
                    String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());
                    map.put(key, tableProcessDwd);
                }
                JdbcUtil.closeConnection(mysqlConn);
            }

            @Override
            public void processElement(JSONObject jsonObj,
                                       BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext context,
                                       Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                ReadOnlyBroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);
                String key = getKey(jsonObj.getString("table"), jsonObj.getString("type"));
                TableProcessDwd tableProcessDwd = state.get(key);

                if (tableProcessDwd == null) {  // 如果状态中没有查到, 则去 map 中查找
                    tableProcessDwd = map.get(key);
                    if (tableProcessDwd != null) {
                        log.info("在 map 中查找到 " + key);
                    }
                } else {
                    log.info("在 状态 中查找到 " + key);
                }
                if (tableProcessDwd != null) { // 这条数据找到了对应的配置信息
                    JSONObject data = jsonObj.getJSONObject("data");
                    out.collect(Tuple2.of(data, tableProcessDwd));
                }
            }

            @Override
            public void processBroadcastElement(TableProcessDwd tableProcessDwd,
                                                BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context,
                                                Collector<Tuple2<JSONObject, TableProcessDwd>> collector)
                    throws Exception {
                BroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);
                String key = getKey(tableProcessDwd.getSourceTable(), tableProcessDwd.getSourceType());

                if ("d".equals(tableProcessDwd.getOp())) {
                    // 删除状态
                    state.remove(key);
                    // map中的配置也要删除
                    map.remove(key);
                } else {
                    // 更新或者添加状态
                    state.put(key, tableProcessDwd);
                }
            }
            private String getKey(String table, String type) {
                return table + ":" + type;
            }
        });
    }


    private SingleOutputStreamOperator<TableProcessDwd> readTableProcess(StreamExecutionEnvironment env) {
        // useSSL=false
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("gmall.table_process_dwd") // set captured table
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial()) // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1) // 并行度设置为 1
                .map(new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");
                        TableProcessDwd tp;
                        if ("d".equals(op)) {
                            tp = obj.getObject("before", TableProcessDwd.class);
                        } else {
                            tp = obj.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);

                        return tp;
                    }
                })
                .setParallelism(1);
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try {
                            JSONObject obj = JSON.parseObject(value);
                            String db = obj.getString("database");
                            String type = obj.getString("type");
                            String data = obj.getString("data");


                            return "gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type))
                                    && data != null
                                    && data.length() > 2;

                        } catch (Exception e) {
                            log.warn("不是正确的 json 格式的数据: {}", value);
                            return false;
                        }

                    }
                })
                .map(JSON::parseObject);
    }

}
