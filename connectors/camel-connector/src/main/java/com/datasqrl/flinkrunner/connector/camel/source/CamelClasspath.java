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
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.apache.flink.annotation.Internal;

/**
 * What the job's classpath must hold for Camel: the component of every endpoint, and Camel jars of
 * the version the connector is built against. Camel needs every component at its core's exact
 * version.
 */
@Internal
public final class CamelClasspath {

  /** The Camel version the connector is built against. */
  public static final String CAMEL_VERSION = builtCamelVersion();

  private static final String VERSION_RESOURCE = "camel.properties";
  private static final String VERSION_PROPERTY = "camel.version";
  // Where Camel's default component resolver looks a component up by name.
  private static final String COMPONENT_DESCRIPTORS =
      "META-INF/services/org/apache/camel/component/";
  private static final String MANIFEST = "META-INF/MANIFEST.MF";
  // Every Apache Camel jar names itself this way, e.g. "Camel :: JMS".
  private static final String CAMEL_TITLE_PREFIX = "Camel :: ";
  private static final String JAR_ENTRY_SEPARATOR = "!/";
  private static final char SCHEME_END = ':';
  private static final String PLACEHOLDER_START = "{{";

  private CamelClasspath() {}

  /**
   * The component an endpoint URI names, the part before its first colon. Empty when a property
   * placeholder gives it, which only Camel resolves.
   */
  public static Optional<String> scheme(String uri) {
    int end = uri.indexOf(SCHEME_END);
    String scheme = end < 0 ? uri : uri.substring(0, end);
    return scheme.contains(PLACEHOLDER_START) ? Optional.empty() : Optional.of(scheme);
  }

  /** Whether the classpath has a Camel component of that name. */
  public static boolean hasComponent(String name, ClassLoader classLoader) {
    return classLoader.getResource(COMPONENT_DESCRIPTORS + name) != null;
  }

  /**
   * Camel jars on the classpath whose version is not {@link #CAMEL_VERSION}, as "jar (version)".
   */
  public static List<String> otherCamelVersions(ClassLoader classLoader) {
    List<String> others = new ArrayList<>();
    try {
      for (URL url : Collections.list(classLoader.getResources(MANIFEST))) {
        Attributes attributes = manifest(url).getMainAttributes();
        String title = attributes.getValue(Attributes.Name.IMPLEMENTATION_TITLE);
        String version = attributes.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
        if (title != null
            && title.startsWith(CAMEL_TITLE_PREFIX)
            && !CAMEL_VERSION.equals(version)) {
          others.add(jarName(url) + " (" + version + ")");
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read the manifests on the job's classpath", e);
    }
    return others;
  }

  /** The error for a component that {@code user} needs and the classpath doesn't have. */
  public static String missingComponent(String name, String user) {
    return "Camel component '"
        + name
        + "', needed by "
        + user
        + ", is not on the job's classpath. Add the Camel component that provides it to the UDF"
        + " path, at Camel "
        + CAMEL_VERSION
        + ".";
  }

  /** A component is missing at runtime. Retrying can't help, so the source fails straight away. */
  public static final class MissingComponentException extends IllegalStateException {
    public MissingComponentException(String name, String user) {
      super(missingComponent(name, user));
    }
  }

  private static Manifest manifest(URL url) throws IOException {
    try (InputStream in = url.openStream()) {
      return new Manifest(in);
    }
  }

  private static String jarName(URL manifest) {
    String path = manifest.getPath();
    int entry = path.lastIndexOf(JAR_ENTRY_SEPARATOR);
    String jar = entry < 0 ? path : path.substring(0, entry);
    return jar.substring(jar.lastIndexOf('/') + 1);
  }

  private static String builtCamelVersion() {
    try (InputStream in = CamelClasspath.class.getResourceAsStream(VERSION_RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException(VERSION_RESOURCE + " is missing from the connector jar");
      }
      Properties properties = new Properties();
      properties.load(in);
      return properties.getProperty(VERSION_PROPERTY);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read " + VERSION_RESOURCE, e);
    }
  }
}
