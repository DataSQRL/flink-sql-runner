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
package com.datasqrl.flinkrunner.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class CursorSourceTest {

  private final CursorSource<String, Instant, String> source = new InstantSource();
  private final CursorSource<String, Long, String> longSource = new LongSource();

  @Test
  void defaultRecordsSplitsIterableArrayAndSingleBodies() {
    assertThat(source.records(exchangeWithBody(List.of("a", "b")))).containsExactly("a", "b");
    assertThat(source.records(exchangeWithBody(new String[] {"a"}))).containsExactly("a");
    assertThat(source.records(exchangeWithBody("a"))).containsExactly("a");
    assertThat(source.records(exchangeWithBody(null))).isEmpty();
  }

  @Test
  void defaultRewindSubtractsFromTemporalPositions() {
    assertThat(source.rewind(Instant.parse("2026-01-01T00:10:00Z"), Duration.ofMinutes(10)))
        .isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Test
  void defaultRewindRejectsOtherPositions() {
    assertThatThrownBy(() -> longSource.rewind(5L, Duration.ofMinutes(1)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static Exchange exchangeWithBody(Object body) {
    Exchange exchange = new DefaultExchange(new DefaultCamelContext());
    exchange.getMessage().setBody(body);
    return exchange;
  }

  private static class InstantSource implements CursorSource<String, Instant, String> {
    @Override
    public Instant initial() {
      return Instant.EPOCH;
    }

    @Override
    public Request request(Instant position) {
      return Request.to("direct:test");
    }

    @Override
    public String map(String record) {
      return record;
    }

    @Override
    public Instant position(String record) {
      return Instant.EPOCH;
    }
  }

  private static class LongSource implements CursorSource<String, Long, String> {
    @Override
    public Long initial() {
      return 0L;
    }

    @Override
    public Request request(Long position) {
      return Request.to("direct:test");
    }

    @Override
    public String map(String record) {
      return record;
    }

    @Override
    public Long position(String record) {
      return 0L;
    }
  }
}
