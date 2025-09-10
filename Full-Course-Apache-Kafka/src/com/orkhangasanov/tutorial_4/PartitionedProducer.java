package com.orkhangasanov.tutorial_4;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class PartitionedProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            String key = "key_" + (i % 3); // 3 different keys - 0, 1, 2
            String value = "Message " + i + " from " + key;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("partitioned-topic", key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent to partition %d with key %s%n", metadata.partition(), key);
                } else {
                    exception.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
