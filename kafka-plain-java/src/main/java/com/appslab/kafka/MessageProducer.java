package com.appslab.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class MessageProducer {

    private static final String TOPIC = "my-kafka-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Map<String, Object> config = Map.of(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try(Producer<Long, String> producer = new KafkaProducer<>(config, new LongSerializer(), new StringSerializer())){

            for (int i = 0;; i++) {
                Long key = (long) i;
                String message = "this is message " + key;

                producer.send(new ProducerRecord<>(TOPIC, key, message));
                System.out.println(">>>>>>>>>>");

                // log a confirmation once the message is written
                System.out.println("sent msg " + key);
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Could not start producer: " + e);
        }
    }
}
