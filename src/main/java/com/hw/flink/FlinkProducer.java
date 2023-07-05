package com.hw.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class FlinkProducer {
    public static void main(String[] args) throws Exception {

        // 配置flink运行时环境，并生成数据流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        // 开启producer事务无法发送数据
//        env.enableCheckpointing(10);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000); //Gap after which next checkpoint can be written.
//        env.getCheckpointConfig().setCheckpointTimeout(4000); //Checkpoints have to complete within 4secs
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); //Only 1 checkpoints can be executed at a time
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 这里会导致程序阻塞，原因未知
//        env.enableCheckpointing(20, CheckpointingMode.EXACTLY_ONCE);
        ArrayList<String> wordLists = new ArrayList<>();
        wordLists.add("hello");
        wordLists.add("world");
        DataStreamSource<String> source = env.fromCollection(wordLists);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092");


        // 创建事务的生产者
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>("first",
                (KafkaSerializationSchema<String>) (s, aLong) -> {
                    // 指定key的序列化
                    return new ProducerRecord<>("first", s.getBytes());
                }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        flinkKafkaProducer.setWriteTimestampToKafka(true);
        source.addSink(flinkKafkaProducer);
        env.registerJobListener(new JobListener() {
            @Override
            public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {

            }

            @Override
            public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {

            }
        });
        env.execute();
    }
}
