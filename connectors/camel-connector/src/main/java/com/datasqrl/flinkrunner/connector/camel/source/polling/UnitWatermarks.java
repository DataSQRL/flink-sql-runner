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
package com.datasqrl.flinkrunner.connector.camel.source.polling;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;

/**
 * The emitter side of the event-time protocol for cursor and file mode. Every record of a unit
 * carries the unit's timestamp T. When the unit completes with new rows, the source emits
 * watermark(T) and marks itself idle until the next unit's first record. A unit without rows emits
 * no watermark, so an unchanged poll never closes a snapshot.
 */
@Internal
public final class UnitWatermarks {

  private final boolean enabled;
  private boolean idle;

  public UnitWatermarks(boolean enabled) {
    this.enabled = enabled;
  }

  public void beforeRecord(SourceOutput<?> output) {
    if (idle) {
      output.markActive();
      idle = false;
    }
  }

  public void unitComplete(SourceOutput<?> output, long timestamp, boolean emittedRows) {
    if (!enabled) {
      return;
    }
    if (emittedRows) {
      output.emitWatermark(new Watermark(timestamp));
    }
    output.markIdle();
    idle = true;
  }
}
