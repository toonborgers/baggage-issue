package com.example.demo;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

@AutoConfiguration(
    before = KafkaAutoConfiguration.class,
    after = MicrometerTracingAutoConfiguration.class)
public class KafkaTracerConfiguration {

  private final KafkaProperties properties;

  KafkaTracerConfiguration(KafkaProperties properties) {
    this.properties = properties;
  }

  @Bean
  TracingRecordInterceptor tracingRecordInterceptor(
      Tracer tracer, Propagator propagator, ObservationRegistry observationRegistry) {
    return new TracingRecordInterceptor(tracer, propagator, observationRegistry);
  }

  @Bean
  ProducerFactory<?, ?> kafkaProducerFactory(
      ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers,
      Tracer tracer,
      Propagator propagator,
      ObservationRegistry observationRegistry) {

    var factory = new DefaultKafkaProducerFactory<>(this.properties.buildProducerProperties());
    var transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
    if (transactionIdPrefix != null) {
      factory.setTransactionIdPrefix(transactionIdPrefix);
    }
    customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
    return new TracingProducerFactory<>(factory, tracer, propagator, observationRegistry);
  }
}
