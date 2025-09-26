package com.orkhangasanov.tutorial_5;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class AdvancedProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        /*
              Reliability settings:

              1.Acknowledgments (acks)
              acks=0: Fire-and-forget, no confirmation. Fastest, but risky.
              acks=1: Leader acknowledges once it writes the message. Safer.
              acks=all: All replicas acknowledge before success. Safest but slower.

              2.Retries

              3.Idempotence

        */

        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // safest
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // exactly-once

        /* Performance settings
              1.Compression
              2.Linger time
              3.Batching
         */
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // 16KB batch
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5"); // wait 5ms to batch more records
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress messages

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            String value = "Advanced message number " + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("test-topic", key, value);

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
