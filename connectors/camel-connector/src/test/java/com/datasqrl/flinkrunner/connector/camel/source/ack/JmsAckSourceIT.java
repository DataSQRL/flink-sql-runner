/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.flinkrunner.connector.camel.source.FailOnceAfterCheckpoint;
import com.datasqrl.flinkrunner.connector.camel.source.TestJmsBroker;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Runs the source against an embedded ActiveMQ broker, through camel-jms and two components built
 * on it. The broker holds a message until the source commits it, so a message still on the queue
 * after a run was never committed.
 */
class JmsAckSourceIT {

  private static final String QUEUE = "shipments";
  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();

  private TestJmsBroker broker;

  @BeforeEach
  void startBroker() throws Exception {
    broker = new TestJmsBroker();
  }

  @AfterEach
  void stopBroker() throws Exception {
    broker.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"jms", "amqp", "activemq6"})
  void redeliversUntilACheckpointCommits(String component) throws Exception {
    broker.send(QUEUE, "a", "b", "c");

    // Without checkpoints nothing is safe to commit, so every message stays for redelivery.
    assertEquals(
        Set.of("a", "b", "c"), run(component, false, new Configuration(), stream -> stream));
    assertEquals(3, broker.depth(QUEUE));

    // The same messages are delivered again, and the final checkpoint commits them.
    assertEquals(
        Set.of("a", "b", "c"), run(component, true, new Configuration(), stream -> stream));
    awaitEmptyQueue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"jms", "amqp", "activemq6"})
  void failoverRedeliversMessagesNoCheckpointCovered(String component) throws Exception {
    broker.send(QUEUE, "a", "b", "c");
    FAILED_ONCE.set(false);

    // The map is chained to the source, so the first message fails the job before any
    // checkpoint can cover it. The broker must deliver it again to the restarted reader.
    Set<String> records =
        run(
            component,
            true,
            FailOnceAfterCheckpoint.withOneRestart(),
            stream ->
                stream.map(
                    value -> {
                      if (!FAILED_ONCE.getAndSet(true)) {
                        throw new IllegalStateException("boom");
                      }
                      return value;
                    }));

    assertTrue(FAILED_ONCE.get());
    assertEquals(Set.of("a", "b", "c"), records);
    awaitEmptyQueue();
  }

  @Test
  void commitsWhileMoreMessagesWait() throws Exception {
    // More messages than the reader can hold between the broker and the pipeline.
    int messages = 3_000;
    broker.send(
        QUEUE, IntStream.range(0, messages).mapToObj(Integer::toString).toArray(String[]::new));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(50);

    // The slow map keeps messages waiting inside the reader for as long as it receives, and a
    // commit would take those along. Only pausing at a checkpoint lets everything received get
    // covered and committed while the broker still has more.
    Set<String> records = new HashSet<>();
    boolean committedInBetween = false;
    try (CloseableIterator<String> results =
        env.fromSource(source("jms"), WatermarkStrategy.noWatermarks(), "camel")
            .map(
                value -> {
                  Thread.sleep(1);
                  return value;
                })
            .executeAndCollect()) {
      while (results.hasNext()) {
        records.add(results.next());
        long depth = broker.depth(QUEUE);
        committedInBetween |= depth > 0 && depth < messages;
      }
    }

    assertTrue(committedInBetween, "nothing was committed until the queue ran dry");
    assertEquals(messages, records.size());
    awaitEmptyQueue();
  }

  private Set<String> run(
      String component,
      boolean checkpointing,
      Configuration config,
      UnaryOperator<DataStream<String>> pipeline)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    if (checkpointing) {
      env.enableCheckpointing(50);
    }

    try (CloseableIterator<String> results =
        pipeline
            .apply(env.fromSource(source(component), WatermarkStrategy.noWatermarks(), "camel"))
            .executeAndCollect()) {
      List<String> records = CollectionUtil.iteratorToList(results);
      assertEquals(records.size(), Set.copyOf(records).size(), "duplicates in " + records);
      return Set.copyOf(records);
    }
  }

  private CamelAckSource<String> source(String component) throws Exception {
    return CamelAckSource.builder(
            new BodyAckSource(broker.endpointUri(component, QUEUE), body -> body))
        .receiveTimeout(Duration.ofMillis(500))
        .stopAfterEmptyPolls(3)
        .build();
  }

  private void awaitEmptyQueue() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (broker.depth(QUEUE) > 0) {
      if (System.nanoTime() > deadline) {
        fail(broker.depth(QUEUE) + " messages were never committed");
      }
      Thread.sleep(50);
    }
  }
}
