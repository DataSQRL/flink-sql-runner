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

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/** Conversions from raw values to the Java types a source's records may hold. */
public final class Types {

  /**
   * ISO-8601 with an offset of {@code Z}, {@code +00:00} or {@code +0000}, as Salesforce writes.
   */
  private static final DateTimeFormatter OFFSET_DATE_TIME =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .appendPattern("[XXX][XX]")
          .toFormatter();

  private static final String ELLIPSIS = "...";

  private Types() {}

  /**
   * The value rounded half-up to {@code scale}, or {@code null} for a missing, null or blank value.
   *
   * @throws ArithmeticException if the rounded value has more than {@code precision} digits
   */
  public static BigDecimal decimal(JsonNode value, int precision, int scale) {
    if (isAbsent(value)) {
      return null;
    }
    return value.isNumber()
        ? fit(value.decimalValue(), precision, scale)
        : decimal(value.asText(), precision, scale);
  }

  /**
   * The value rounded half-up to {@code scale}, or {@code null} for a null or blank value.
   *
   * @throws ArithmeticException if the rounded value has more than {@code precision} digits
   */
  public static BigDecimal decimal(String value, int precision, int scale) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return fit(new BigDecimal(value.trim()), precision, scale);
  }

  /**
   * An ISO-8601 timestamp with an offset of {@code Z}, {@code +00:00} or {@code +0000}, or {@code
   * null} for a missing or null value.
   */
  public static Instant instant(JsonNode value) {
    if (isAbsent(value)) {
      return null;
    }
    return OFFSET_DATE_TIME.parse(value.asText(), Instant::from);
  }

  /** The date in {@code pattern}, a {@link DateTimeFormatter} pattern, or {@code null}. */
  public static LocalDate date(String value, String pattern) {
    if (value == null) {
      return null;
    }
    return LocalDate.parse(value, DateTimeFormatter.ofPattern(pattern));
  }

  /**
   * The value as text, cut to {@code maxLength} characters, for log messages. A {@code null} value
   * is {@code "null"}.
   */
  public static String truncate(Object value, int maxLength) {
    if (value == null) {
      return String.valueOf((Object) null);
    }
    String text = value.toString();
    return text.length() <= maxLength ? text : text.substring(0, maxLength) + ELLIPSIS;
  }

  private static boolean isAbsent(JsonNode value) {
    return value == null || value.isNull() || value.isMissingNode();
  }

  private static BigDecimal fit(BigDecimal value, int precision, int scale) {
    BigDecimal scaled = value.setScale(scale, RoundingMode.HALF_UP);
    if (scaled.precision() > precision) {
      throw new ArithmeticException(
          value + " does not fit DECIMAL(" + precision + ", " + scale + ")");
    }
    return scaled;
  }
}
