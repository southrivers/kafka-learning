package com.hw.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerNonKeyPartition {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer会根据这个参数来决定数据的超时发送
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 60000);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 5; i++) {
                    // 经测试不指定key的情况下是batch.size或者linger.ms之后随机选择下一个分区进行发送
                    producer.send(new ProducerRecord<>("first", "value" + i), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println(metadata);
                        }
                    });
                }
            }
        }).start();

        Thread.currentThread().join();
    }
}
