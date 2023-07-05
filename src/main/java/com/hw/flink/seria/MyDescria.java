package com.hw.flink.seria;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MyDescria implements KafkaDeserializationSchema<String> {

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        long offset = record.offset();
        int partition = record.partition();
        String value = new String(record.value());
        JSONObject parse = new JSONObject();
        parse.put("value", value);
        parse.put("partition", partition);
        parse.put("offset", offset);
        return parse.toString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
