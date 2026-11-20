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
package com.datasqrl.flinkrunner.connector.camel.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.camel.Request;
import org.apache.camel.Exchange;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

class UserSourceTest {

  public record Out(String a) {}

  public static class Cursor implements CursorSource<Out, Long, String> {
    @Override
    public Long initial() {
      return 0L;
    }

    @Override
    public Request request(Long position) {
      return Request.to("direct:unused");
    }

    @Override
    public Out map(String record) {
      return new Out(record);
    }

    @Override
    public Long position(Out record) {
      return 0L;
    }
  }

  public static class Ack implements AckSource<Out> {
    @Override
    public String endpoint() {
      return "direct:unused";
    }

    @Override
    public Out map(Exchange exchange) {
      return null;
    }
  }

  public static class File implements FileSource<Out, String> {
    @Override
    public String location() {
      return "file:/unused";
    }

    @Override
    public Iterable<String> records(Exchange file) {
      return null;
    }

    @Override
    public Out map(String record) {
      return new Out(record);
    }
  }

  public static class NeedsArgument extends Ack {
    public NeedsArgument(String argument) {}
  }

  public static class AckAndFile extends Ack implements FileSource<Out, String> {
    @Override
    public String location() {
      return "file:/unused";
    }

    @Override
    public Iterable<String> records(Exchange file) {
      return null;
    }

    @Override
    public Out map(String record) {
      return null;
    }
  }

  public static class Lines implements FileSource<String, String> {
    @Override
    public String location() {
      return "file:/unused";
    }

    @Override
    public Iterable<String> records(Exchange file) {
      return null;
    }

    @Override
    public String map(String record) {
      return record;
    }
  }

  @Test
  void loadsEachMode() {
    assertThat(load(Cursor.class))
        .extracting(UserSource::mode, UserSource::recordClass)
        .containsExactly(SourceMode.CURSOR, Out.class);
    assertThat(load(Ack.class))
        .extracting(UserSource::mode, UserSource::recordClass)
        .containsExactly(SourceMode.ACK, Out.class);
    assertThat(load(File.class))
        .extracting(UserSource::mode, UserSource::recordClass)
        .containsExactly(SourceMode.FILE, Out.class);
    assertThat(load(File.class).instance()).isInstanceOf(File.class);
  }

  @Test
  void rejectsAMissingClass() {
    assertThatThrownBy(() -> load("no.such.Source"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("no.such.Source");
  }

  @Test
  void rejectsAClassWithoutANoArgumentConstructor() {
    assertThatThrownBy(() -> load(NeedsArgument.class))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("no-argument constructor");
  }

  @Test
  void rejectsAClassWithoutAContract() {
    assertThatThrownBy(() -> load(Object.class))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("CursorSource, AckSource or FileSource");
  }

  @Test
  void rejectsAClassWithTwoContracts() {
    assertThatThrownBy(() -> load(AckAndFile.class))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("exactly one");
  }

  @Test
  void rejectsAnOutputThatIsNotARecord() {
    assertThatThrownBy(() -> load(Lines.class))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must be a record");
  }

  private static UserSource load(Class<?> sourceClass) {
    return load(sourceClass.getName());
  }

  private static UserSource load(String className) {
    return UserSource.load(className, UserSourceTest.class.getClassLoader());
  }
}
