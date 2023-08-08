package com.example.demo;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

class TracingProducer<K, V> implements Producer<K, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TracingProducer.class);


  private final Producer<K, V> producer;
  private final Tracer tracer;
  private final Propagator propagator;
  private final ObservationRegistry observationRegistry;

  TracingProducer(
      Producer<K, V> producer,
      Tracer tracer,
      Propagator propagator,
      ObservationRegistry observationRegistry) {
    this.producer = producer;
    this.tracer = tracer;
    this.propagator = propagator;
    this.observationRegistry = observationRegistry;
  }

  @Override
  public void initTransactions() {
    producer.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    producer.beginTransaction();
  }

  @Override
  @Deprecated
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
      throws ProducerFencedException {
    producer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  @Override
  public void sendOffsetsToTransaction(
      Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata)
      throws ProducerFencedException {
    producer.sendOffsetsToTransaction(offsets, groupMetadata);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    producer.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    producer.abortTransaction();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return Observation.createNotStarted("Kafka producer", observationRegistry)
        .contextualName("Produce to " + record.topic())
        .observe(() -> producer.send(inject(record)));
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    return producer.send(inject(record), callback);
  }

  private ProducerRecord<K, V> inject(ProducerRecord<K, V> record) {
    propagator.inject(
        tracer.currentTraceContext().context(), record.headers(), TracingProducer::add);
    return record;
  }

  private static void add(Headers carrier, String key, String value) {
    LOGGER.info("Adding header [{}: {}]", key, value);
    carrier.add(key, value.getBytes());
  }

  @Override
  public void flush() {
    producer.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return producer.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return producer.metrics();
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public void close(Duration timeout) {
    producer.close(timeout);
  }
}
