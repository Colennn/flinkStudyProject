package com.ym.gmall.function;

import com.alibaba.fastjson.JSONObject;
import com.ym.gmall.common.bean.TableProcessDim;
import com.ym.gmall.common.constant.Constant;
import com.ym.gmall.common.utils.HBaseUtil;
import com.ym.gmall.common.utils.RedisUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection hbaseConn;

//    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getConnection();
//        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(hbaseConn);
//        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {

        // HBase 存放维度信息
        String op = value.f1.getOp();
        if ("delete".equals(op)) {
            log.info("写入 HBase，数据：{}", value.f0);
            HBaseUtil.delRow(hbaseConn,
                    Constant.HBASE_NAMESPACE,
                    value.f1.getSinkTable(),
                    value.f1.getSinkRowKey());
        } else {
            log.info("从 HBase 里面删除数据：{}", value.f0);
            HBaseUtil.putRow(hbaseConn,
                    Constant.HBASE_NAMESPACE,
                    value.f1.getSinkTable(),
                    value.f1.getSinkRowKey(),
                    value.f1.getSinkFamily(),
                    value.f0);
        }

        // redis 做旁路缓存
        if ("delete".equals(op) || "update".equals(op)) {
            log.info("遇到数据更新或删除，从 Redis 里面删除：{}", value.f0);
            String key = RedisUtil.getKey(value.f1.getSinkTable(), value.f1.getSinkRowKey());
//            jedis.del(key); // TODO 这里一开始还没太看懂，在写入  HBase 之前就已经缓存到 Redis 了
        }
    }
}
