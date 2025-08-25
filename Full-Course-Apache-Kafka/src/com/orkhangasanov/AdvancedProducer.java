package com.orkhangasanov;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class AdvancedProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Reliability settings
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // safest
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // exactly-once

        // Performance settings
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // 16KB batch
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5"); // wait 5ms to batch more records
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress messages

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            String value = "Advanced message number " + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("partitioned-topic", key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent message with key=%s to partition=%d at offset=%d%n",
                            key, metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
