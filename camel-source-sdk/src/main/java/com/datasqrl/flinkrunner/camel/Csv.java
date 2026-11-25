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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Parses CSV files with commons-csv. */
public final class Csv {

  /** RFC 4180 with no header row: every line is a record. */
  public static final CSVFormat NO_HEADER = CSVFormat.DEFAULT;

  private Csv() {}

  /**
   * The file's records, each a list of its fields, read lazily. The file is decoded with the
   * exchange's {@link Exchange#CHARSET_NAME} property, or UTF-8 when it is not set.
   */
  public static Iterable<List<String>> parse(Exchange file, CSVFormat format) {
    String charsetName = file.getProperty(Exchange.CHARSET_NAME, String.class);
    Charset charset = charsetName == null ? StandardCharsets.UTF_8 : Charset.forName(charsetName);
    InputStream body = file.getMessage().getBody(InputStream.class);
    try {
      CSVParser parser = CSVParser.parse(new InputStreamReader(body, charset), format);
      return new StreamedRecords<>(parser.stream().map(CSVRecord::toList).iterator(), parser);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
