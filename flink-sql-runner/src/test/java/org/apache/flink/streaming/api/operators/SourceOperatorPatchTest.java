/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.metrics.groups.InternalSourceSplitMetricGroup;
import org.apache.flink.runtime.util.EnvironmentInformation;

import com.datasqrl.flinkrunner.CliRunner;
import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/** Guards the FLINK-40093 patch, see {@link org.apache.flink.streaming.api.operators}. */
class SourceOperatorPatchTest {

    /**
     * The version FLINK-40093 is patched against. The fix ships in Flink 2.3.1, so a bump past this
     * makes the whole patch obsolete.
     */
    private static final String PATCHED_FLINK_VERSION = "2.3.0";

    @Test
    void givenPatchedClasses_whenResolvedAtRuntime_thenTheyShadowFlinkRuntime() {
        assertThat(codeSourceOf(SourceOperator.class))
                .as("SourceOperator must resolve to our patched copy, not the flink-runtime jar")
                .isEqualTo(codeSourceOf(CliRunner.class));

        assertThat(codeSourceOf(InternalSourceSplitMetricGroup.class))
                .as(
                        "InternalSourceSplitMetricGroup must resolve to our patched copy, not the"
                                + " flink-runtime jar")
                .isEqualTo(codeSourceOf(CliRunner.class));
    }

    @Test
    void givenFlinkVersion_whenItMovesPastThePatchedOne_thenDropThePatch() {
        assertThat(EnvironmentInformation.getVersion())
                .as(
                        "FLINK-40093 is fixed in Flink 2.3.1+. Delete the vendored SourceOperator,"
                                + " InternalSourceSplitMetricGroup, their package-info files and this"
                                + " test.")
                .isEqualTo(PATCHED_FLINK_VERSION);
    }

    private static URL codeSourceOf(Class<?> type) {
        return type.getProtectionDomain().getCodeSource().getLocation();
    }
}
