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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/** An empty jar whose manifest identifies it the way Camel's own jars do. */
public final class FakeCamelJar {

  private static final String MANIFEST_VERSION = "1.0";

  private FakeCamelJar() {}

  public static Path write(Path dir, String name, String title, String version) throws IOException {
    Manifest manifest = new Manifest();
    Attributes attributes = manifest.getMainAttributes();
    attributes.put(Attributes.Name.MANIFEST_VERSION, MANIFEST_VERSION);
    attributes.put(Attributes.Name.IMPLEMENTATION_TITLE, title);
    attributes.put(Attributes.Name.IMPLEMENTATION_VERSION, version);
    Path jar = dir.resolve(name);
    try (OutputStream out = Files.newOutputStream(jar);
        JarOutputStream ignored = new JarOutputStream(out, manifest)) {
      return jar;
    }
  }
}
