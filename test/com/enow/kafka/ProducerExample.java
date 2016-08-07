package com.enow.kafka;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
public class ProducerExample {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        // "test" topic에 "Hello, World!" 메시지 전송
        KeyedMessage<String, String> message = new KeyedMessage<String, String>("test", "Hello, World!");
        producer.send(message);
        // "Hello, World!" 메시지 10개를 한 번에 전송
        // List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
        // for (int i = 0; i < 10; i++) {
        //     messages.add(new KeyedMessage<String, String>("test", "Hello, World!"));
        // }
        // producer.send(messages);

        producer.close();
    }
}
