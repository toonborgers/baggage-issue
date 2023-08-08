package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;

class TestConfiguration {
    @Bean
     NewTopic topic() {
        return new NewTopic(DemoTest.TOPIC, 1, (short) 1);
    }

    @Bean
    TestListener testListener() {
        return Mockito.mock(TestListener.class);
    }
}
