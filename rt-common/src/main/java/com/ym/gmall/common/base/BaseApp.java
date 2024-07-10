package com.ym.gmall.common.base;

import com.ym.gmall.common.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    // TODO 需要写一个 mockStart() 方法，来模拟 kafka 数据源
    // 1. 自己真的搭一个kafka 数据源，但是还不会 kafka 怎么用的。（先快速看一下 kafka 怎么玩的）
    //      按照教程里面，至少要装 kafka、mysql、HBase、FlinkCDC，工程量还是有点大的
    // 2. 使用 mysql 或者其他的数据源代替。
    // 感觉还是第一个靠谱
    // TODO 这里看要不要用 docker 把环境搭好，创建一个快捷指令一键启动
    // FlinkCDC 比较简单，还是不要花费太多时间了

    /**
     * 设置一个默认的启动方法，后面的子类只需要实现 handle 方法即可
     * @param port
     * @param parallelism
     * @param ckAndGroupId
     * @param topic
     */
    public void start(int port, int parallelism, String ckAndGroupId, String topic) {

        // 1. 环境准备

        // 1.1 获取流处理环境
        System.setProperty("HADOOP_USER_NAME", "ym");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2 设置流处理环境变量
        env.setParallelism(parallelism);
        /**
         * 从 Flink 1.13 版本开始，社区改进了 state backend 的公开类，进而帮助用户更好理解本地状态存储和 checkpoint 存储的区分。
         * 只要学习最新的 HashMapStateBackend 和 RocksDBStateBackend，对比两者差异
         * 在启动 CheckPoint 机制时，状态会随着 CheckPoint 而持久化，以防止数据丢失、保障恢复时的一致性。
         * 状态内部的存储格式、状态在 CheckPoint 时如何持久化以及持久化在哪里均取决于选择的 State Backend。
         * 总结：状态后端是指 Flink 运行时如何存储状态，以及如何持久化状态。
         */
        env.setStateBackend(new HashMapStateBackend()); // 设置状态后端，数据都存在 HashMap 里面，这种用来调试比较方便
        env.enableCheckpointing(5000); // 每隔 5 秒生成一个检查点
        /**
         * 1. EXACTLY_ONCE：确保检查点是精确一次的，这是默认值。
         *      实现原理：两阶段提交（2PC）上游预先提交状态变更，如果上下游都准备好了才进行正式提交。
         *      对数据准确性多了一层保证，对系统资源的要求较高
         * 2. AT_LEAST_ONCE：确保检查点是至少一次的。
         *      实现原理：当消息被处理后，系统会向数据源发送一个确认，表示数据已处理完成。
         *      有可能因为网络延迟、系统故障导致数据重复多次处理。
         */
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // 精准一次
        env.getCheckpointConfig().setCheckpointTimeout(10000); // ck 超时时间
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // ck 最大并行数
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000); // ck 之前的最小间隔
        env.getCheckpointConfig().setCheckpointStorage("hdfs://没有安装 hdfs");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION); // job 取消时的 ck 保留策略

        // 获取kafka 的 topic 里面获取数据流
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);
        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 2.执行具体逻辑
        handle(env, stream);

        // 3.执行 Job
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
