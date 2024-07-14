package com.ym.gmall.dws;

import com.ym.gmall.common.base.BaseApp;
import com.ym.gmall.common.bean.TrafficPageViewBean;
import com.ym.gmall.common.constant.Constant;
import com.ym.gmall.common.function.DorisMapFunction;
import com.ym.gmall.common.utils.FlinkSinkUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析数据, 封装 pojo 中
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(stream);
        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> result = windowAndAgg(beanStream);

        // 3. 写出到 doris 中
        writeToDoris(result);
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        stream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_traffic_vc_ch_ar_is_new_page_view_window", "dws_traffic_vc_ch_ar_is_new_page_view_window"));
        return null;
    }
}
