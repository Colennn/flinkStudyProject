package com.ym.gmall.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ym.gmall.common.base.BaseApp;
import com.ym.gmall.common.constant.Constant;
import com.ym.gmall.common.utils.DateFormatUtil;
import com.ym.gmall.common.utils.FlinkSinkUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;

import java.util.HashMap;
import java.util.Map;

import static com.ym.gmall.common.constant.Constant.TOPIC_LOG;

public class DwdBaseLog extends BaseApp {
    private static final Logger log = LoggerFactory.getLogger(DwdBaseLog.class);

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(2001, 1, "dwd_base_log", TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1. etl 读取神策生产的 kafka 数据
            // 神策生成的 json 数据，通过 flumn 采集在 kafka 生产数据，再通过 flink 消费这些数据
            // 神策支持直接生成 kafka 数据，到我们这里的数据是直接进入到不同的 topic 了，至于是否kafka 是否支持这样的功能就不知道了
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
        // 2. 纠正新老用户
        SingleOutputStreamOperator<JSONObject> validatedStream = valideNewCustom(etlStream);
        // 3. 日志数据分流，这里用标签
        Map<String, DataStream<JSONObject>> streams = splitStream(validatedStream);
        // 4. 写入到不同的 topic
        writeToKafka(streams);
    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams
                .get(START)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        streams
                .get(ERR)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        streams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

        streams
                .get(ACTION)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {

        OutputTag<JSONObject> errTag = new OutputTag<>(ERR);
        OutputTag<JSONObject> displayTag = new OutputTag<>(DISPLAY);
        OutputTag<JSONObject> actionTag = new OutputTag<>(ACTION);
        OutputTag<JSONObject> pageTag = new OutputTag<>(PAGE);

        SingleOutputStreamOperator<JSONObject> startStream = validatedStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject obj, 
                                       ProcessFunction<JSONObject, JSONObject>.Context ctx,
                                       Collector<JSONObject> out) throws Exception {
                JSONObject common = obj.getJSONObject("common");
                Long ts = obj.getLong("ts");
                // 1. 启动
                JSONObject start = obj.getJSONObject("start");
                if (start != null) {
                    out.collect(obj);
                }

                // 2. 曝光
                JSONArray displays = obj.getJSONArray("displays");
                if (displays != null) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        display.putAll(common);
                        display.put("ts", ts);
                        ctx.output(displayTag, display);
                    }
                    // 删除displays
                    obj.remove("displays");
                }

                // 3. 活动
                JSONArray actions = obj.getJSONArray("actions");
                if (actions != null) {
                    for (int i = 0; i < actions.size(); i++) {
                        JSONObject action = actions.getJSONObject(i);
                        action.putAll(common);
                        ctx.output(actionTag, action);
                    }

                    // 删除displays
                    obj.remove("actions");
                }

                // 4. err
                JSONObject err = obj.getJSONObject("err");
                if (err != null) {
                    ctx.output(errTag, obj);
                    obj.remove("err");
                }

                // 5. 页面
                JSONObject page = obj.getJSONObject("page");
                if (page != null) {
                    ctx.output(pageTag, obj);
                }
            }
        });

        Map<String, DataStream<JSONObject>> streams = new HashMap<>();
        streams.put(START, startStream);
        streams.put(ERR, startStream.getSideOutput(errTag));
        streams.put(DISPLAY, startStream.getSideOutput(displayTag));
        streams.put(ACTION, startStream.getSideOutput(actionTag));
        streams.put(PAGE, startStream.getSideOutput(pageTag));
        return streams;
    }

    private SingleOutputStreamOperator<JSONObject> valideNewCustom(SingleOutputStreamOperator<JSONObject> etlStream) {
        return etlStream
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> firstVisitState;
                    public void open(Configuration parameters) throws Exception {
                        // TODO 获取状态，判断是否是新用户
                        firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("state", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               KeyedProcessFunction<String, JSONObject, JSONObject>.Context context,
                                               Collector<JSONObject> collector) throws Exception {
                        // TODO 这里不太理解如何维护一个用户的新旧状态。 答：用Flink状态来维护用户新旧状态
                        JSONObject common = obj.getJSONObject("common");
                        String isNew = common.getString("is_new");

                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        String firstVisitDate = firstVisitState.value();

                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                firstVisitState.update(today);
                            } else if (!today.equals(firstVisitDate)) {
                                common.put("isNew", "0"); // 把新用户修正为老用户
                            }
                        } else {
                            if (firstVisitDate == null) {
                                firstVisitState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                            }
                        }

                        collector.collect(obj);
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                try {
                    JSONObject.parse(s);
                    return true;
                } catch (Exception e) {
                    log.info("不是正确的 json：{}", s);
                    return false;
                }
            }
        }).map(JSON::parseObject);
    }
}
