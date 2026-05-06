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
package com.datasqrl.flinkrunner.connector.datagen.metronome.split;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Serializer for the metronome split progress state. */
public final class MetronomeSplitSerializer implements SimpleVersionedSerializer<MetronomeSplit> {

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(MetronomeSplit split) throws IOException {
    var out = new DataOutputSerializer(16);
    out.writeLong(split.lastEmittedNumber());
    out.writeLong(split.startTimestampSec());

    return out.getCopyOfBuffer();
  }

  @Override
  public MetronomeSplit deserialize(int version, byte[] serialized) throws IOException {
    var in = new DataInputDeserializer(serialized);

    return new MetronomeSplit(in.readLong(), in.readLong());
  }
}
