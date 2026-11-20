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
package com.datasqrl.flinkrunner.connector.camel.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Serializes the enumerator state of the cursor and file modes: the splits not yet assigned to a
 * reader, each written with the mode's split serializer and its version.
 */
@Internal
public final class SplitListSerializer<SplitT> implements SimpleVersionedSerializer<List<SplitT>> {

  private final SimpleVersionedSerializer<SplitT> splitSerializer;

  public SplitListSerializer(SimpleVersionedSerializer<SplitT> splitSerializer) {
    this.splitSerializer = splitSerializer;
  }

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(List<SplitT> unassigned) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(unassigned.size());
      for (SplitT split : unassigned) {
        byte[] serialized = splitSerializer.serialize(split);
        out.writeInt(splitSerializer.getVersion());
        out.writeInt(serialized.length);
        out.write(serialized);
      }
    }
    return bytes.toByteArray();
  }

  @Override
  public List<SplitT> deserialize(int version, byte[] serialized) throws IOException {
    if (version != 1) {
      throw new IOException("Unknown Camel enumerator state serializer version " + version);
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
      int count = in.readInt();
      List<SplitT> unassigned = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        int splitVersion = in.readInt();
        byte[] split = new byte[in.readInt()];
        in.readFully(split);
        unassigned.add(splitSerializer.deserialize(splitVersion, split));
      }
      return unassigned;
    }
  }
}
