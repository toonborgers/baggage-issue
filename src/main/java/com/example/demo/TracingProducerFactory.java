package com.example.demo;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.ProducerFactory;

public class TracingProducerFactory<K, V> implements ProducerFactory<K, V> {

  private final ProducerFactory<K, V> producerFactory;
  private final Tracer tracer;
  private final Propagator propagator;
  private final ObservationRegistry observationRegistry;

  public TracingProducerFactory(
      ProducerFactory<K, V> producerFactory,
      Tracer tracer,
      Propagator propagator,
      ObservationRegistry observationRegistry) {
    this.producerFactory = producerFactory;
    this.tracer = tracer;
    this.propagator = propagator;
    this.observationRegistry = observationRegistry;
  }

  @Override
  public Producer<K, V> createProducer() {
    return new TracingProducer<>(
        producerFactory.createProducer(), tracer, propagator, observationRegistry);
  }
}
