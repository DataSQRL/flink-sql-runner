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

import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

/**
 * The single split of a Camel source. In ack mode the source system tracks delivery, so the split
 * carries no read position: unacknowledged messages are redelivered by the system after a restore.
 */
public final class AckSplit implements SourceSplit {

  private final String splitId;

  public AckSplit() {
    this(CamelSplitEnumerator.SPLIT_ID);
  }

  AckSplit(String splitId) {
    this.splitId = splitId;
  }

  @Override
  public String splitId() {
    return splitId;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof AckSplit other && splitId.equals(other.splitId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(splitId);
  }

  @Override
  public String toString() {
    return "AckSplit{" + splitId + "}";
  }
}
