package com.ym.gmall.common.mock;

import com.alibaba.fastjson.JSON;
import com.ym.gmall.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.ym.gmall.common.constant.Constant.TOPIC_DB;
import static com.ym.gmall.common.constant.Constant.TOPIC_DIM;

public class readOds extends BaseApp {
    public static void main(String[] args) {
        new readOds().start(1012, 1, "test_readOds", TOPIC_DIM);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
