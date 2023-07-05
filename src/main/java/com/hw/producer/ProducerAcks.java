package com.hw.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAcks {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 这里是设置应答的机制，从服务端来看，并不是从客户端来看的，也就是说客户端可以阻塞，也可以不阻塞
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
//        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION
        // 对于replica.lag.time.max.ms和min.insync.replicas这俩参数均是在server.properties里面配置并生效的
        // 其分别用于指定fellow的超时心跳时间，以及isr队列中最少要有多少个fellow处于存活状态，如果存活的fellower的副本数量少于
        // 参数的配置，在写入的时候会直接抛出副本数量不够的异常
//        properties.put(ProducerConfig.)

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "ack"+i));
        }

        producer.close();
    }
}
