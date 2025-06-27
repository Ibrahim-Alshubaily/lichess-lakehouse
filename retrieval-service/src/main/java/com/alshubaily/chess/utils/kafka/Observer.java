package com.alshubaily.chess.utils.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class Observer {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaConsumer<String, String> consumer;
    private final List<Consumer<String>> handlers = new CopyOnWriteArrayList<>();
    private Thread worker;

    public Observer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.SERVER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "extract-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void register(Consumer<String> handler) {
        handlers.add(handler);
    }

    public void start() {
        worker = new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, String> record : records) {
                        for (Consumer<String> handler : handlers) {
                            handler.accept(record.value());
                        }
                    }
                }
            } catch (WakeupException ignored) {
            } finally {
                consumer.close();
            }
        }, "kafka-observer");

        worker.setDaemon(true);
        worker.start();
    }

    public void close() {
        consumer.wakeup();
        try {
            worker.join();
        } catch (InterruptedException ignored) {
        }
    }
}
