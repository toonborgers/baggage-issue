package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@Import(KafkaTracerConfiguration.class)
public class DemoApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @KafkaListener(
            topics = "testtopic",
            groupId = "test-qsdfqsdf")
    public void handle(ConsumerRecord item){

        var message = new StringBuilder("Received message " + item.value())
                .append("\n\tHeaders:");
        item.headers().forEach(h -> message.append("\n\t%s: %s".formatted(h.key(), new String(h.value()))));

        LOGGER.info(message.toString());
    }
}
