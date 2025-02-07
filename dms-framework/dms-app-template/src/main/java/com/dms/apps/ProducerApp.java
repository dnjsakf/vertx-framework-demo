package com.dms.apps;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@SuppressWarnings("unchecked")
public class ProducerApp {

    public static void main(String[] args) {

        ProducerApp kafkaApp = new ProducerApp();

        CompletableFuture<Void>[] futures = new CompletableFuture[1];
        for (int i = 0; i < futures.length; i++) {
            futures[i] = CompletableFuture.runAsync(() -> {
                kafkaApp.produce("topic-navilog2-acs-post-json");
            });
        }
        
        // CompletableFuture<Void>[] futures = IntStream.range(0, 5)
        //     .mapToObj(i -> CompletableFuture.runAsync(() -> {
        //         kafkaConsume();
        //     }))
        //     .toArray(CompletableFuture[]::new);

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures);
        allFutures.join(); // 모든 task가 완료될 때까지 기다립니다.
    }

    public void produce(String topic){
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        String username = "consumer2";
        String password = "consumer-zjstbaj2~";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            username,
            password
        ));

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Runnable sendTask = () -> {
            String key = "key";
            String value = "value";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Sent message to partition " + metadata.partition() + " with offset " + metadata.offset());
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        // 1초마다 실행
        executorService.scheduleAtFixedRate(sendTask, 0, 1, TimeUnit.SECONDS);

        // Runtime 종료 후 producer와 executorService 종료
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.close();
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }));
    }
    
}
