package com.example.demo;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

public class TracingRecordInterceptor implements RecordInterceptor {
  private final Tracer tracer;
  private final Propagator propagator;
  private final ObservationRegistry observationRegistry;
  private final ThreadLocal<Span> span = new ThreadLocal<>();
  private final ThreadLocal<Tracer.SpanInScope> spanInScope = new ThreadLocal<>();
  private final ThreadLocal<Observation> observation = new ThreadLocal<>();
  private final ThreadLocal<Observation.Scope> observationScope = new ThreadLocal<>();

  public TracingRecordInterceptor(
      Tracer tracer, Propagator propagator, ObservationRegistry observationRegistry) {
    this.tracer = tracer;
    this.propagator = propagator;
    this.observationRegistry = observationRegistry;
  }

  @Override
  public ConsumerRecord intercept(ConsumerRecord record, Consumer consumer) {
    span.set(
        propagator
            .extract(
                record.headers(),
                (carrier, key) -> {
                  var header = carrier.lastHeader(key);
                  return header == null ? null : new String(header.value());
                })
            .start());
    spanInScope.set(tracer.withSpan(span.get()));
    observation.set(
        Observation.start("Kafka consumer", observationRegistry)
            .contextualName("Consume from " + record.topic()));
    observationScope.set(observation.get().openScope());
    return record;
  }

  @Override
  public void afterRecord(ConsumerRecord record, Consumer consumer) {
    if (observationScope.get() != null) {
      observationScope.get().close();
      observationScope.remove();
    }
    if (observation.get() != null) {
      observation.get().stop();
      observation.remove();
    }
    if (spanInScope.get() != null) {
      spanInScope.get().close();
      spanInScope.remove();
    }
    if (span.get() != null) {
      span.get().end();
      span.remove();
    }
  }
}
