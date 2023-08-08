package com.example.demo;

import org.springframework.kafka.annotation.KafkaListener;

import static com.example.demo.DemoTest.TOPIC;

interface TestListener {
    @KafkaListener(
            topics = TOPIC,
            groupId = "test-cg")
    void handle(String item);
}
