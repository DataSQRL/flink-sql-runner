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
package com.datasqrl.flinkrunner.connector.camel.source.cursor;

import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollSettings;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.InstantiationUtil;

/** Everything a cursor-mode source needs at runtime. Shipped to the task managers. */
record CursorSettings<OUT, POS, IN, R>(
    CursorSource<OUT, POS, IN> source,
    RecordConverter<OUT, R> converter,
    Map<String, String> componentOptions,
    PollSettings poll,
    boolean fullRefresh,
    Duration lookback,
    TypeSerializer<POS> positionSerializer,
    byte[] startPosition)
    implements Serializable {

  /** {@code cursor.start} when set, otherwise the source's {@code initial()}. */
  POS startPositionOrInitial() throws IOException {
    if (startPosition == null) {
      return source.initial();
    }
    return InstantiationUtil.deserializeFromByteArray(positionSerializer, startPosition);
  }
}
