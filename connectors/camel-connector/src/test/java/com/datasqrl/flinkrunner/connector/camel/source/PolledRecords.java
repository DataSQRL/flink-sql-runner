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
package com.datasqrl.flinkrunner.connector.camel.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

/**
 * Runs a source and collects what each record looked like downstream: its value, its timestamp, and
 * the watermark the next operator had seen when the record arrived.
 */
public final class PolledRecords {

  public record Polled(String value, long timestamp, long watermark) {}

  private static final AtomicInteger COLLECTED = new AtomicInteger();

  private PolledRecords() {}

  /**
   * Collects until {@code done} holds for what arrived so far, then cancels the job, or until a
   * bounded job finishes.
   */
  public static List<Polled> collect(
      StreamExecutionEnvironment env,
      Source<String, ?, ?> source,
      UnaryOperator<DataStream<String>> pipeline,
      Predicate<List<Polled>> done)
      throws Exception {
    DataStream<String> stream =
        pipeline.apply(env.fromSource(source, WatermarkStrategy.noWatermarks(), "camel"));
    List<Polled> polled = new ArrayList<>();
    try (CloseableIterator<String> results = stream.process(new Describe()).executeAndCollect()) {
      while (!done.test(polled) && results.hasNext()) {
        String[] parts = results.next().split("\\|", -1);
        polled.add(new Polled(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2])));
        COLLECTED.incrementAndGet();
      }
    }
    return polled;
  }

  public static List<Polled> collect(
      StreamExecutionEnvironment env, Source<String, ?, ?> source, Predicate<List<Polled>> done)
      throws Exception {
    return collect(env, source, stream -> stream, done);
  }

  /** Starts counting the records {@link #collect} hands out from zero. */
  public static void resetCollected() {
    COLLECTED.set(0);
  }

  /** How many records {@link #collect} has handed out since the reset, for other test threads. */
  public static int collectedSoFar() {
    return COLLECTED.get();
  }

  private static final class Describe extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
      out.collect(value + "|" + ctx.timestamp() + "|" + ctx.timerService().currentWatermark());
    }
  }
}
