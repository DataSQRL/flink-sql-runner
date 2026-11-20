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

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Serializes {@link CursorSplit}, writing the position with the position type's serializer. */
final class CursorSplitSerializer<POS> implements SimpleVersionedSerializer<CursorSplit<POS>> {

  private final TypeSerializer<POS> positionSerializer;

  CursorSplitSerializer(TypeSerializer<POS> positionSerializer) {
    this.positionSerializer = positionSerializer;
  }

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(CursorSplit<POS> split) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(64);
    out.writeUTF(split.splitId());
    out.writeLong(split.lastT());
    out.writeLong(split.unitT());
    out.writeBoolean(split.position() != null);
    if (split.position() != null) {
      TypedValues.write(positionSerializer, split.position(), out);
    }
    return out.getCopyOfBuffer();
  }

  @Override
  public CursorSplit<POS> deserialize(int version, byte[] serialized) throws IOException {
    if (version != 1) {
      throw new IOException("Unknown CursorSplit serializer version " + version);
    }
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    String splitId = in.readUTF();
    long lastT = in.readLong();
    long unitT = in.readLong();
    POS position = in.readBoolean() ? TypedValues.read(positionSerializer, in) : null;
    return new CursorSplit<>(splitId, position, lastT, unitT);
  }
}
