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
package com.datasqrl.flinkrunner.stdlib.typesafe;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A failure while classifying a single row.
 *
 * <p>Messages never contain state, instructions or any other request or response content, since
 * they end up in the {@code _error} column, in exceptions and in logs.
 */
@Getter
public class ClassifyException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /** Error classes, used as the metric tag of the {@code errors} counter. */
  @Getter
  @RequiredArgsConstructor
  public enum ErrorClass {
    RETRYABLE_EXHAUSTED("retryable_exhausted"),
    HTTP_4XX("http_4xx"),
    CONTRACT("contract"),
    /** Configuration errors such as 401/403 that fail the job regardless of {@code on_error}. */
    FATAL("fatal");

    private final String tag;
  }

  private final ErrorClass errorClass;
  private final String code;

  public ClassifyException(ErrorClass errorClass, String code, String message) {
    super(code + ": " + message);
    this.errorClass = errorClass;
    this.code = code;
  }

  static ClassifyException contract(String message) {
    return new ClassifyException(ErrorClass.CONTRACT, "CONTRACT", message);
  }

  boolean isFatal() {
    return errorClass == ErrorClass.FATAL;
  }
}
