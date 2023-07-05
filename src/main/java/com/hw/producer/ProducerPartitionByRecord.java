package com.hw.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerPartitionByRecord {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 这个参数是用来指定从broker获取元数据的最大的阻塞时间，比如下面指定了一个不存在的分区，这就会导致获取不到对应的元数据，因此这里就会超时
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", 3, "", "value" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null != exception) {
                        exception.printStackTrace();
                    } else {
                        System.out.println(metadata);
                    }
                }
            });
        }

        producer.close();
    }
}
