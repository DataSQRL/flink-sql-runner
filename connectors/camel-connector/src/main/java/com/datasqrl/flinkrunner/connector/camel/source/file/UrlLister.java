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

import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.polling.RateLimiter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.spi.PropertyConfigurer;

/**
 * Reads one published document over HTTP(S). A listing is one conditional GET: {@code 304 Not
 * Modified} means the version already read is still current. Otherwise the version is a SHA-256 of
 * the content, so a document republished with the same content isn't read again.
 *
 * <p>The validators ({@code ETag}, {@code Last-Modified}) are kept in memory only. After a restore
 * the first GET downloads the document, and the seen-set decides.
 */
final class UrlLister implements FileLister {

  private final CamelRuntime runtime;
  private final String url;
  private final RateLimiter rateLimiter;
  private final long timeoutMs;
  private final Set<String> listed = new HashSet<>();

  private boolean fetched;
  private String etag;
  private String lastModified;
  private String lastVersion;

  UrlLister(CamelRuntime runtime, String url, RateLimiter rateLimiter, long timeoutMs) {
    this.runtime = runtime;
    this.url = url;
    this.rateLimiter = rateLimiter;
    this.timeoutMs = timeoutMs;
  }

  @Override
  public void startListing() {
    listed.clear();
    fetched = false;
  }

  @Override
  public ListedFile next(Predicate<String> skip) throws IOException {
    if (fetched) {
      return DONE;
    }
    rateLimiter.acquire();
    Exchange response =
        runtime.sendWithRetries(
            url,
            request -> {
              request.getMessage().setHeader(Exchange.HTTP_METHOD, "GET");
              if (etag != null) {
                request.getMessage().setHeader("If-None-Match", etag);
              }
              if (lastModified != null) {
                request.getMessage().setHeader("If-Modified-Since", lastModified);
              }
            },
            timeoutMs);
    if (response == null) {
      return null;
    }
    fetched = true;
    Integer status = response.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
    if (status != null && status == 304) {
      if (lastVersion != null) {
        listed.add(lastVersion);
      }
      return DONE;
    }
    byte[] content = response.getMessage().getBody(byte[].class);
    if (content == null) {
      content = new byte[0];
    }
    etag = response.getMessage().getHeader("ETag", String.class);
    lastModified = response.getMessage().getHeader("Last-Modified", String.class);
    lastVersion = url + "@sha256:" + sha256(content);
    listed.add(lastVersion);
    if (skip.test(lastVersion)) {
      return DONE;
    }
    response.getMessage().setBody(content);
    response.getMessage().setHeader(Exchange.FILE_NAME, url);
    return new ListedFile(lastVersion, url, response);
  }

  @Override
  public Set<String> listed() {
    return Set.copyOf(listed);
  }

  @Override
  public void release(ListedFile file) {}

  /** Lets {@code 304 Not Modified} through as a response instead of a failed request. */
  static void configure(Endpoint endpoint) throws Exception {
    PropertyConfigurer configurer = endpoint.getComponent().getEndpointPropertyConfigurer();
    if (configurer == null
        || !configurer.configure(
            endpoint.getCamelContext(), endpoint, "okStatusCodeRange", "200-299,304", true)) {
      throw new IllegalArgumentException(
          "File mode reads URLs with camel-http, which is not on the classpath for "
              + endpoint.getEndpointBaseUri());
    }
  }

  private static String sha256(byte[] content) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
