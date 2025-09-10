package com.orkhangasanov.tutorial_7;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ReliableConsumer {
    private static final String MAIN_TOPIC = "payments";
    private static final String DLQ_TOPIC = "payments-dlq";

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "reliable-group");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(Collections.singleton(MAIN_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processMessage(record.value());
                        System.out.printf("Processed: %s%n", record.value());
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + record.value());
                        // retry once
                        try {
                            processMessage(record.value());
                        } catch (Exception ex) {
                            System.err.println("Sending to DLQ: " + record.value());
                            producer.send(new ProducerRecord<>(DLQ_TOPIC, record.key(),
                                    record.value()));
                        }
                    }
                }
                consumer.commitSync();
            }
        } finally {
            consumer.close();
            producer.close();
        }
    }

    private static void processMessage(String message) throws Exception {
        if (message.contains("FAIL")) {
            throw new Exception("Simulated processing failure");
        }
        // otherwise "success"
    }
}