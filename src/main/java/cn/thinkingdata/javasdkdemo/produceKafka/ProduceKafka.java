package cn.thinkingdata.javasdkdemo.produceKafka;

import cn.thinkingdata.tga.javasdk.Consumer;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class ProduceKafka implements Consumer {
    private Properties props = new Properties();
    private Producer<String,String> producer;
    private String topic;

    public ProduceKafka(String Server, String topic) {
        props.put("bootstrap.servers", Server);
        props.put("compression.type","lz4");
        props.put("batch.size","262144");
        props.put("linger.ms","100");
        props.put("buffer.memory","33554432");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("max.request.size", 104857600);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.topic = topic;
        producer = new KafkaProducer<>(props);
    }

    public ProduceKafka(String Server){
        this(Server,"tga_data_collector");
    }

    @Override
    public void add(Map<String, Object> message) {
        try {
            String partitionKey;
            Object distinctId = message.get("#distinct_id");
            Object accountId = message.get("#account_id");
            if(distinctId != null && distinctId.toString().length() > 0){
                partitionKey = distinctId.toString();
            }else if(accountId != null && accountId.toString().length() > 0){
                partitionKey = accountId.toString();
            }else {
                partitionKey = UUID.randomUUID().toString();
            }
            String value = JSON.toJSONStringWithDateFormat(message, "yyyy-MM-dd HH:mm:ss.SSS");
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partitionKey, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to add data.", e);
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        this.flush();
        producer.close();
    }

    public void setProps(String key,Object value){
        this.props.put(key,value);
    }

    public void setTopic(String topic){
        this.topic = topic ;
    }
}
