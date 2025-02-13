package com.dms.apps;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

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
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            username,
            password
        ));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // 16 KB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Runnable sendTask = () -> {
            String key = "key";
            String value = "{\"data\":\"ping\"}";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            try {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Sent message to partition " + metadata.partition() + " with offset " + metadata.offset());
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        // 1초마다 실행
        executorService.scheduleAtFixedRate(sendTask, 0, 100, TimeUnit.MILLISECONDS);

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

    public void produceFile(String topic){
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

        String username = "consumer2";
        String password = "consumer-zjstbaj2~";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            username,
            password
        ));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // 16 KB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Runnable sendTask = () -> {
            // String key = "key";
            // String value = "{\"data\":\"ping\"}";
            // ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            BufferedReader br = null;
            try {
                // 파일 읽기 및 Kafka에 비동기 전송
                br = new BufferedReader(new FileReader("C:\\dev\\workspace\\java\\vertx-framework-demo\\dms-framework\\dms-app-template\\docker\\backup\\sample.data"));
                String line;
                while ((line = br.readLine()) != null) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                        }
                    });
                }
                // producer.send(record, (metadata, exception) -> {
                //     if (exception != null) {
                //         exception.printStackTrace();
                //     }
                // });
                // RecordMetadata metadata = producer.send(record).get();
                // System.out.println("Sent message to partition " + metadata.partition() + " with offset " + metadata.offset());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if( br != null ){
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        // 1초마다 실행
        // executorService.scheduleAtFixedRate(sendTask, 0, 1, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(sendTask, 0, 10, TimeUnit.MILLISECONDS);

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
