package com.hw.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class ProducerParamTest {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        // 这个参数用来控制sender线程发送的延时时间，实现的效果就是批量发送数据，这里的单位是毫秒
//        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000000);
//        properties
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 5; i++) {
            System.out.println(new Date());
            producer.send(new ProducerRecord<String,String>("first", "value" + i));
        }

        Thread.currentThread().join();
        // 主动调用close方法会让sender清空recordaccumulator里面的数据
        producer.close();
    }
}
