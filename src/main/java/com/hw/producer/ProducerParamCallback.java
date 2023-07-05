package com.hw.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerParamCallback {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // 采用这种api的方式是异步的，因此建议开辟一个子线程来处理数据，并且让主线程阻塞
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 5; i++) {
                    producer.send(new ProducerRecord<>("first", "value" + i), new Callback() {
                        // 这里callback会在producer接收到broker返回的ack的信息之后再执行，并不是丢到recordacculator中就完事了
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.println(metadata);
                            }
                        }
                    });
                }
            }
        }).start();

        Thread.currentThread().join();
        producer.close();
    }
}
