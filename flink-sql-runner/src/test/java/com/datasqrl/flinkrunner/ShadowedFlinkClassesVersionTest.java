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
package com.datasqrl.flinkrunner;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.junit.jupiter.api.Test;

class ShadowedFlinkClassesVersionTest {

  private static final String SHADOWED_FROM_FLINK_VERSION = "2.3.0";

  private static final String RESYNC_INSTRUCTIONS =
      """
      Flink was upgraded from %1$s to %2$s, but the classes under \
      flink-sql-runner/src/main/java/org/apache/flink/ are copies of Flink %1$s classes. They \
      override Flink's own classes at runtime (the uber jar sits in /opt/flink/lib, ahead of \
      flink-dist), so a stale copy fails with NoSuchMethodError or silently reverts upstream \
      changes.

      INSTRUCTIONS FOR THE AI AGENT OR DEVELOPER FIXING THIS FAILURE. Do not just bump \
      SHADOWED_FROM_FLINK_VERSION; resync every copy first:
      1. List the copies: \
      find flink-sql-runner/src/main/java/org/apache/flink -name '*.java' ! -name package-info.java
      2. For each copy, read the package-info.java next to it. It names the FLINK-xxxxx ticket \
      the copy backports, or explains a permanent customization.
      3. Recover our patch: diff the copy against the same file from the Flink %1$s sources jar \
      of the artifact that owns the package (flink-runtime, flink-avro, ...), e.g. \
      ~/.m2/repository/org/apache/flink/flink-runtime/%1$s/flink-runtime-%1$s-sources.jar \
      (fetch it with mvn dependency:sources if missing).
      4. If Flink %2$s already contains the ticket's fix (check the ticket's Fix Version in \
      https://issues.apache.org/jira and the %2$s source), delete the copy and its \
      package-info.java. Copies without a ticket are permanent customizations: never delete them.
      5. Otherwise replace the copy with the Flink %2$s version of the file, re-apply the patch \
      from step 3, and update the Flink version named in its package-info.java.
      6. Remove the dependencies marked FIXME in flink-sql-runner/pom.xml once no remaining \
      copy needs them.
      7. Set SHADOWED_FROM_FLINK_VERSION in %3$s to %2$s.
      8. Run mvn clean install -Pdev, which also runs the integration tests.
      """;

  @Test
  void shadowedFlinkClassesMatchFlinkVersion() {
    var flinkVersion = EnvironmentInformation.getVersion();

    assertThat(flinkVersion)
        .as(
            RESYNC_INSTRUCTIONS.formatted(
                SHADOWED_FROM_FLINK_VERSION, flinkVersion, getClass().getSimpleName()))
        .isEqualTo(SHADOWED_FROM_FLINK_VERSION);
  }
}
