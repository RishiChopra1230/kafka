package com.appslab.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class MessageConsumer {
    private static final String TOPIC = "my-kafka-topic";
    // The group ID is a unique identified for each consumer group
    private static final String GROUP_ID = "my-group-id";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Every time we consume a message from kafka, we need to "commit" - that is, acknowledge
        // receipts of the messages. We can set up an auto-commit at regular intervals, so that
        // this is taken care of in the background

        final Map<String, Object> config = Map.of(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
                GROUP_ID_CONFIG, GROUP_ID,
                ENABLE_AUTO_COMMIT_CONFIG, "true",
                AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000",
                KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongDeserializer",
                VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");


        try (KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(config)) {
            // Subscribe this consumer to the same topic that we wrote messages to earlier
            consumer.subscribe(Collections.singletonList(TOPIC));

            // run an infinite loop where we consume and print new messages to the topic
            while (true) {
                // The consumer.poll method checks and waits for any new messages to arrive for the subscribed topic
                // in case there are no messages for the duration specified in the argument (3000 ms// in this case), it returns an empty list
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(3000));
                System.out.println("Fetched " + records.count() + " records");
                for (ConsumerRecord<Long, String> record : records) {
                    System.out.println("Received: " + record.key() + ":" + record.value());
                }
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (Exception e) {
                    break;
                }
            }
        }
    }
}
