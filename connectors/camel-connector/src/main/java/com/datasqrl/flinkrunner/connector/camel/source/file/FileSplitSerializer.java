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
package com.datasqrl.flinkrunner.connector.camel.source.file;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Serializes {@link FileSplit}. */
final class FileSplitSerializer implements SimpleVersionedSerializer<FileSplit> {

  static final FileSplitSerializer INSTANCE = new FileSplitSerializer();

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(FileSplit split) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(256);
    out.writeUTF(split.splitId());
    out.writeLong(split.lastT());
    out.writeLong(split.unitT());
    out.writeBoolean(split.unitKey() != null);
    if (split.unitKey() != null) {
      writeString(out, split.unitKey());
    }
    out.writeLong(split.lastUnitRows());
    out.writeInt(split.seen().size());
    for (Map.Entry<String, Long> version : split.seen().entrySet()) {
      writeString(out, version.getKey());
      out.writeLong(version.getValue());
    }
    return out.getCopyOfBuffer();
  }

  @Override
  public FileSplit deserialize(int version, byte[] serialized) throws IOException {
    if (version != 1) {
      throw new IOException("Unknown FileSplit serializer version " + version);
    }
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    String splitId = in.readUTF();
    long lastT = in.readLong();
    long unitT = in.readLong();
    String unitKey = in.readBoolean() ? readString(in) : null;
    long lastUnitRows = in.readLong();
    int count = in.readInt();
    Map<String, Long> seen = new HashMap<>(count * 2);
    for (int i = 0; i < count; i++) {
      seen.put(readString(in), in.readLong());
    }
    return new FileSplit(splitId, seen, lastT, unitT, unitKey, lastUnitRows);
  }

  // writeUTF is limited to 64 KB, which a long path or URL plus version could approach.
  private static void writeString(DataOutputSerializer out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputDeserializer in) throws IOException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
