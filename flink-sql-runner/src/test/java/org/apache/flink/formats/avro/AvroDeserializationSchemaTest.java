/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package org.apache.flink.formats.avro;

import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class AvroDeserializationSchemaTest {

    private static final Address ADDRESS = TestDataGenerator.generateRandomAddress(new Random());
    private static final byte[] CORRUPT_BYTES = {0x0, 0x1, 0x2, 0x3, 0x4};

    @Test
    void testGenericRecordFailureRecovery() throws IOException {
        AvroDeserializationSchema<GenericRecord> deserSchema =
                AvroDeserializationSchema.forGeneric(ADDRESS.getSchema());

        byte[] msgBytes = AvroTestUtils.writeRecord(ADDRESS, Address.getClassSchema());
        deserSchema.deserialize(msgBytes);

        try {
            deserSchema.deserialize(CORRUPT_BYTES);
            fail("Should fail when deserializing corrupt bytes.");
        } catch (Exception ignored) {
        }

        GenericRecord result = deserSchema.deserialize(msgBytes);
        assertThat(result).isNotNull();
        assertThat(result.get("num")).isEqualTo(ADDRESS.getNum());
        assertThat(result.get("state").toString()).isEqualTo(ADDRESS.getState());
        assertThat(result.get("city").toString()).isEqualTo(ADDRESS.getCity());
        assertThat(result.get("street").toString()).isEqualTo(ADDRESS.getStreet());
    }

    @Test
    void testSpecificRecordFailureRecovery() throws IOException {
        AvroDeserializationSchema<Address> deserSchema =
                AvroDeserializationSchema.forSpecific(Address.class);

        byte[] msgBytes = AvroTestUtils.writeRecord(ADDRESS, Address.getClassSchema());
        deserSchema.deserialize(msgBytes);

        try {
            deserSchema.deserialize(CORRUPT_BYTES);
            fail("Should fail when deserializing corrupt bytes.");
        } catch (Exception ignored) {
        }

        Address result = deserSchema.deserialize(msgBytes);
        assertThat(result).isEqualTo(ADDRESS);
    }
}
