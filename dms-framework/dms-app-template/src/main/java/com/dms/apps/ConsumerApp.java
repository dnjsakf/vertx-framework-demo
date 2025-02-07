package com.dms.apps;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@SuppressWarnings("unchecked")
public class ConsumerApp {

    public static void main(String[] args) {

        CompletableFuture<Void>[] futures = new CompletableFuture[5];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = CompletableFuture.runAsync(() -> {
                consume("topic-navilog2-acs-post-json");
            });
        }

        // CompletableFuture<Void>[] futures = IntStream.range(0, 5)
        //     .mapToObj(i -> CompletableFuture.runAsync(() -> {
        //         consume();
        //     }))
        //     .toArray(CompletableFuture[]::new);

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures);
        allFutures.join(); // 모든 task가 완료될 때까지 기다립니다.
    }

    public static void consume(String topic){
        String username = "consumer1";
        String password = "consumer-zjstbaj1!";

        // Kafka Consumer 설정
        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "acs-group");
        props.put("bootstrap.servers", "localhost:9094");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            username,
            password
        ));
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        // 데이터 소비 및 분석
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                // 데이터 분석 로직 추가
                System.out.println(String.format("[%s-%d] Received message: %s", Thread.currentThread().getId(), record.partition(), value));
            }
        }
    }
    
}
