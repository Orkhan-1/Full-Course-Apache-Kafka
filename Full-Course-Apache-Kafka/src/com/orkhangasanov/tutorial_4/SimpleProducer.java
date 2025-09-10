package com.orkhangasanov.tutorial_4;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // Step 1: Producer configuration
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 2: Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Step 3: Create a record to send
        ProducerRecord<String, String> record =
                new ProducerRecord<>("my-first-topic", "Hello from Java 25!");

        // Step 4: Send data (asynchronous)
        producer.send(record);

        // Step 5: Flush and close
        producer.close();
    }
}
