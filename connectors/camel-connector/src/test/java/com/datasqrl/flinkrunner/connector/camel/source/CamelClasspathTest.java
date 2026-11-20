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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CamelClasspathTest {

  private static final ClassLoader NO_CAMEL = new URLClassLoader(new URL[0], null);

  @TempDir Path dir;

  @Test
  void schemeIsThePartBeforeTheFirstColon() {
    assertThat(CamelClasspath.scheme("aws2-sqs://orders?region=us-west-2")).contains("aws2-sqs");
    assertThat(CamelClasspath.scheme("file:/data/in?include=.*\\.csv")).contains("file");
    assertThat(CamelClasspath.scheme("jms:queue:shipments")).contains("jms");
  }

  @Test
  void schemeFromAPlaceholderIsUnknownUntilCamelResolvesIt() {
    assertThat(CamelClasspath.scheme("{{env:SCHEME}}:queue")).isEmpty();
  }

  @Test
  void findsAComponentByItsDescriptor() {
    ClassLoader classpath = getClass().getClassLoader();

    assertThat(CamelClasspath.hasComponent("file", classpath)).isTrue();
    assertThat(CamelClasspath.hasComponent("nosuch", classpath)).isFalse();
    assertThat(CamelClasspath.hasComponent("file", NO_CAMEL)).isFalse();
  }

  @Test
  void camelVersionIsTheOneTheConnectorWasBuiltWith() {
    assertThat(CamelClasspath.CAMEL_VERSION).isEqualTo(new DefaultCamelContext().getVersion());
  }

  @Test
  void otherCamelVersionsIsEmptyForTheConnectorsOwnCamel() {
    assertThat(CamelClasspath.otherCamelVersions(getClass().getClassLoader())).isEmpty();
  }

  @Test
  void otherCamelVersionsListsCamelJarsOfAnotherVersion() throws IOException {
    Path fake = camelJar("camel-fake-1.0.0.jar", "Camel :: Fake", "1.0.0");
    Path same = camelJar("camel-same.jar", "Camel :: Same", CamelClasspath.CAMEL_VERSION);
    Path other = camelJar("not-camel-1.0.0.jar", "Something Else", "1.0.0");

    try (URLClassLoader classpath =
        new URLClassLoader(
            new URL[] {url(fake), url(same), url(other)}, getClass().getClassLoader())) {
      assertThat(CamelClasspath.otherCamelVersions(classpath))
          .containsExactly("camel-fake-1.0.0.jar (1.0.0)");
    }
  }

  @Test
  void missingComponentNamesWhatNeedsItAndTheVersionToAdd() {
    assertThat(CamelClasspath.missingComponent("jms", "Camel source 'a.B'"))
        .contains("Camel component 'jms', needed by Camel source 'a.B'")
        .contains(CamelClasspath.CAMEL_VERSION);
  }

  private Path camelJar(String name, String title, String version) throws IOException {
    return FakeCamelJar.write(dir, name, title, version);
  }

  private static URL url(Path path) throws IOException {
    return path.toUri().toURL();
  }
}
