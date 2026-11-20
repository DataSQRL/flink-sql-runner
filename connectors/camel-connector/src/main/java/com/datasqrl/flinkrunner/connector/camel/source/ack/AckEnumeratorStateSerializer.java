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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** Serializes the enumerator state: the splits not yet assigned to a reader. */
final class AckEnumeratorStateSerializer implements SimpleVersionedSerializer<List<AckSplit>> {

  static final AckEnumeratorStateSerializer INSTANCE = new AckEnumeratorStateSerializer();

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(List<AckSplit> unassigned) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(unassigned.size());
      for (AckSplit split : unassigned) {
        out.writeUTF(split.splitId());
      }
    }
    return bytes.toByteArray();
  }

  @Override
  public List<AckSplit> deserialize(int version, byte[] serialized) throws IOException {
    if (version != 1) {
      throw new IOException("Unknown Camel enumerator state serializer version " + version);
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
      int count = in.readInt();
      List<AckSplit> unassigned = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        unassigned.add(new AckSplit(in.readUTF()));
      }
      return unassigned;
    }
  }
}
