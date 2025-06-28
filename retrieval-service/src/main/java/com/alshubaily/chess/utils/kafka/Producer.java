package com.alshubaily.chess.utils.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public Producer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, value);
        producer.send(record, (_, exception) -> {
            if (exception != null) {
                System.err.println("❌ Failed to publish to " + topic + ": " + exception.getMessage());
            } else {
                System.out.println("📤 Published to " + topic + ": " + value);
            }
        });
    }

    public void close() {
        producer.close();
    }
}
