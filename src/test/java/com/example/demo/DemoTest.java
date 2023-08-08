package com.example.demo;

import io.micrometer.tracing.BaggageInScope;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {DemoApplication.class, TestConfiguration.class})
@Testcontainers
@AutoConfigureObservability
class DemoTest {
    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0")).withKraft();

    @DynamicPropertySource
    public static void setProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private TestListener testListener;
    @Autowired private Tracer tracer;

    static final String TOPIC = "testtopic";

    private static final String BAGGAGE_KEY = "my-baggage-key";
    private static final String BAGGAGE_VALUE = "my-baggage-value";

    private static final String OTHER_BAGGAGE_KEY = "other-baggage-key";
    private static final String OTHER_BAGGAGE_VALUE = "other-baggage-value";

    private final AtomicBoolean isBaggagePassed = new AtomicBoolean(false);
    private final String value = randomUUID().toString();

    @Test
    void test() throws Exception {
        stubListener();

        sendInSpan();

        verifyBaggagePassed();
    }

    private void stubListener() {
        doAnswer(
                invocation -> {
                    try (var baggage = tracer.getBaggage(BAGGAGE_KEY).makeCurrent()) {
                        isBaggagePassed.set(BAGGAGE_VALUE.equals(baggage.get()));
                    }
                    if(isBaggagePassed.get()) {
                        try (var baggage = tracer.getBaggage(OTHER_BAGGAGE_KEY).makeCurrent()) {
                            isBaggagePassed.set(OTHER_BAGGAGE_VALUE.equals(baggage.get()));
                        }
                    }
                    return null;
                })
                .when(testListener)
                .handle(value);
    }

    private void sendInSpan() throws Exception {
        var span = tracer.nextSpan().name("my-trace").start();
        try (var scope = tracer.withSpan(span)) {
            tracer.createBaggage(BAGGAGE_KEY, BAGGAGE_VALUE);
            tracer.createBaggage(OTHER_BAGGAGE_KEY, OTHER_BAGGAGE_VALUE);
            kafkaTemplate.send(TOPIC, value).get();
        }finally {
            span.end();
        }
    }

    private void verifyBaggagePassed() {
        verify(testListener, timeout(2000)).handle(value);
        assertThat(isBaggagePassed).isTrue();
    }

}
