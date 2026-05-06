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
package com.datasqrl.flinkrunner.connector.datagen.metronome.enumerator;

import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplit;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Serializer for enumerator checkpoint state. */
public final class MetronomeEnumeratorStateSerializer
    implements SimpleVersionedSerializer<MetronomeEnumeratorState> {

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(MetronomeEnumeratorState state) throws IOException {
    var out = new DataOutputSerializer(18);
    var pendingSplit = state.getPendingSplit();

    out.writeBoolean(state.isAssigned());
    out.writeBoolean(pendingSplit != null);

    if (pendingSplit != null) {
      out.writeLong(pendingSplit.lastEmittedNumber());
      out.writeLong(pendingSplit.startTimestampSec());
    }

    return out.getCopyOfBuffer();
  }

  @Override
  public MetronomeEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    var in = new DataInputDeserializer(serialized);

    boolean assigned = in.readBoolean();
    boolean hasPendingSplit = in.readBoolean();

    return hasPendingSplit
        ? MetronomeEnumeratorState.pending(new MetronomeSplit(in.readLong(), in.readLong()))
        : MetronomeEnumeratorState.of(assigned);
  }
}
