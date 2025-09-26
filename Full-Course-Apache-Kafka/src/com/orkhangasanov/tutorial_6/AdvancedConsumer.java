package com.orkhangasanov.tutorial_6;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AdvancedConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "advanced-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
        Consumer settings:

        1.Consumer Groups - Consumers join a group with a unique group.id.
        Kafka ensures each partition is read by only one consumer in a group.

        2.Offsets - Partitions are ordered logs; each message has an offset.
        Offsets track a consumer's position.
        Kafka stores offsets in the __consumer_offsets topic for recovery

        3.Rebalancing - When consumers join/leave, partitions are redistributed
        Consumers pause briefly during rebalancing

        4.Auto vs Manual Commit - By default, offsets are committed automatically every 5 seconds
*/


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("test-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: %s from partition %d at offset %d%n",
                            record.value(), record.partition(), record.offset());

                    // Simulate processing
                    processMessage(record.value());
                }

                // commit offsets manually after batch is processed
                consumer.commitSync();
                System.out.println("Offsets committed!");
            }
        } finally {
            consumer.close();
        }
    }

    private static void processMessage(String message) {
        // Saving to database here
    }
}