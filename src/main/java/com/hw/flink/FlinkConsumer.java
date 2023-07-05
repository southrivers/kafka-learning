package com.hw.flink;

import com.hw.flink.seria.MyDescria;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(2);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("first", new MyDescria(), properties);
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), properties);

        DataStreamSource<String> source = environment.addSource(kafkaConsumer);
        source.print();
        environment.execute();
    }
}
