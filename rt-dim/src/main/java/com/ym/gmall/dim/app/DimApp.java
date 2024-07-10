package com.ym.gmall.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ym.gmall.common.base.BaseApp;
import com.ym.gmall.common.bean.TableProcessDim;
import com.ym.gmall.common.utils.HBaseUtil;
import com.ym.gmall.common.utils.JdbcUtil;
import com.ym.gmall.function.HBaseSinkFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
import org.apache.hadoop.hbase.client.Connection;

import java.util.*;

import static com.ym.gmall.common.constant.Constant.*;

@Slf4j
public class DimApp extends BaseApp {

    public static void main(String[] args) {
        new DimApp() // 这里调用基类里面写好的 start()
                .start(9999,    // TODO 端口等环境搭建好再改
                        4,  // 这里为什么给
                        "dim_app",
                        TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1. 从 kafka 消费数据，对数据进行清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 2. 通过 flinkCDC 读取配置表的数据，从 MySQL 读取数据
        SingleOutputStreamOperator<TableProcessDim> configStream = readTableProceess(env);
        // 3. 根据配置表的数据，再 HBase 中建表 TODO 这里用到了富函数，要重点看一下
        //      配置流写入到广播流里面
        configStream = creatHBaseTable(configStream);
        // 4. 主流 connect 配置流，并对关联后的流作处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectStream =
                connect(etlStream, configStream);
        // 5. 删除不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream =
                deleteNotNeedColumns(connectStream);
        // 6. 写出到 HBase 目标表？ TODO 这里的数据在哪里会用到？
        writeToHBase(resultStream);


    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> deleteNotNeedColumns(
            SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectStream) {

        return connectStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(
                    Tuple2<JSONObject, TableProcessDim> jsonObjectTableProcessDimTuple2) throws Exception {
                JSONObject jsonObject = jsonObjectTableProcessDimTuple2.f0;
                ArrayList<String> columns =
                        new ArrayList<>(Arrays.asList(jsonObjectTableProcessDimTuple2.f1.getSinkColumns().split(",")));
                jsonObject.keySet().removeIf(key -> !columns.contains(key));
                return jsonObjectTableProcessDimTuple2;
            }
        });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(
            SingleOutputStreamOperator<JSONObject> etlStream,
            SingleOutputStreamOperator<TableProcessDim> configStream) {

        // 因为要获取配置流里面的数据，所以要创建一个描述符
        MapStateDescriptor<String, TableProcessDim> dimDescripter = new MapStateDescriptor<>("table_process_dim",
                String.class,
                TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastConfigStream = configStream.broadcast(dimDescripter);
        return etlStream.connect(broadcastConfigStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

                    private HashMap<String, TableProcessDim> map;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        map =  new HashMap<>();
                        java.sql.Connection mysqlConn = JdbcUtil.getMysqlConnection();
                        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConn,
                                "select * from gmall2023_config.table_process_dim",
                                TableProcessDim.class,
                                true
                        );

                        for (TableProcessDim tableProcessDim : tableProcessDims) {
                            map.put(tableProcessDim.getSinkRowKey(), tableProcessDim);
                        }

                        JdbcUtil.closeConnection(mysqlConn);

                    }

                    // 处理数据流中的数据
                    @Override
                    public void processElement(JSONObject jsonObject,
                                               BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext,
                                               Collector<Tuple2<JSONObject, TableProcessDim>> collector)
                            throws Exception {
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState =
                                readOnlyContext.getBroadcastState(dimDescripter);
                        String key = jsonObject.getString("table");
                        TableProcessDim tableProcessDim = broadcastState.get(key);

                        if (tableProcessDim == null) {
                            tableProcessDim = map.get(key);
                            if (tableProcessDim != null) {
                                log.info("从主流中获取到配置信息：{}", key);
                            }
                        } else {
                            log.info("从状态中获取到配置信息：{}", key);
                        }

                        if (tableProcessDim != null) {
                            JSONObject data = jsonObject.getJSONObject("data");
                            data.put("op_type", jsonObject.getString("type")); // TODO 这里不知道后面要干什么
                            collector.collect(Tuple2.of(data, tableProcessDim));
                        }


                    }

                    // 处理广播流中的数据
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                                        BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context,
                                                        Collector<Tuple2<JSONObject, TableProcessDim>> collector)
                            throws Exception {
                        BroadcastState<String, TableProcessDim> broadcastState =
                                context.getBroadcastState(dimDescripter);
                        String key = tableProcessDim.getSourceTable();

                        if ("d".equals(tableProcessDim.getOp())) {
                            broadcastState.remove(key);
                            map.remove(key);
                        } else {
                            broadcastState.put(key, tableProcessDim);
                        }

                    }
                });
    }

    /**
     * 输入的元组第一个元素是实际存放到 HBase 和 Redis 的数据，第二个元素是配置对象（记录了这一行数据要存放在哪个表里面）
     *
     * @param resultStream
     */
    private void writeToHBase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultStream) {
        /*
        1. 有没有专门的 HBase 连接器
            没有
        2. sql 有专门的 HBase 连接器, 由于一次只能写到一个表中, 所以也不能把流转成表再写

        3. 自定义sink
         */
        resultStream.addSink(new HBaseSinkFunction());
    }

    private SingleOutputStreamOperator<TableProcessDim> creatHBaseTable(
            SingleOutputStreamOperator<TableProcessDim> stream) {
        return stream.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn;

            @Override
            public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                // c r 建表
                // d 删表
                // u 先删表再建表
                String op = tableProcessDim.getOp();
                if (op.equals("c") || op.equals("r")) {
                    HBaseUtil.createHBaseTable(hbaseConn,
                            HBASE_NAMESPACE,
                            tableProcessDim.getSinkTable(),
                            tableProcessDim.getSinkFamily());
                } else if (op.equals("d")) {
                    HBaseUtil.dropHBaseTable(hbaseConn, HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                } else { // u
                    HBaseUtil.dropHBaseTable(hbaseConn, HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                    HBaseUtil.createHBaseTable(hbaseConn,
                            HBASE_NAMESPACE,
                            tableProcessDim.getSinkTable(),
                            tableProcessDim.getSinkFamily());
                }
                return tableProcessDim;
            }
        }).setParallelism(1);
    }


    private SingleOutputStreamOperator<TableProcessDim> readTableProceess(StreamExecutionEnvironment env) {

        // 下面的配置用于 JDBC
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .username(MYSQL_USER_NAME)
                .password(MYSQL_PASSWORD)
                .databaseList(MYSQL_DATABASE)
                .tableList(MYSQL_TABLE)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())  // 为什么这一句和MySqlSource.<String>builder()对应
                .startupOptions(StartupOptions.initial())
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1)
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String op = jsonObject.getString("op"); // 获取配置表的操作类型
                        TableProcessDim tableProcessDim;
                        if ("r".equals(op)) {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        } else if ("c".equals(op)) {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        } else if ("u".equals(op)) {
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        } else if ("d".equals(op)) {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }).setParallelism(1);

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    String data = jsonObject.getString("data");

                    return ("gmall".equals(database) &&
                            ("insert".equals(type) || "update".equals(type) || "delete".equals(type)) && data != null &&
                            data.length() > 2);


                } catch (Exception e) {
                    log.warn("不是正确的 json 格式：{}", value);
                    return false;
                }
            }
        }).map(JSON::parseObject);
    }
}
