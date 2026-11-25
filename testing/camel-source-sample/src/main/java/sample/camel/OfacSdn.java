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
package sample.camel;

import com.datasqrl.flinkrunner.camel.CamelSource;
import com.datasqrl.flinkrunner.camel.Csv;
import com.datasqrl.flinkrunner.camel.FileSource;
import java.util.List;
import org.apache.camel.Exchange;

/** The OFAC sanctions list, a published CSV snapshot, in file mode. */
@CamelSource
public class OfacSdn implements FileSource<OfacSdn.Sdn, List<String>> {

  public record Sdn(long entNum, String name, String type, String programs, String remarks) {}

  public String location() {
    return "https://sanctionslistservice.ofac.treas.gov"
        + "/api/PublicationPreview/exports/SDN.CSV";
  }

  public Iterable<List<String>> records(Exchange file) {
    return Csv.parse(file, Csv.NO_HEADER);
  }

  public Sdn map(List<String> c) {
    if (c.size() < 12) return null; // trailing junk line
    return new Sdn(
        Long.parseLong(c.get(0).trim()),
        nul(c.get(1)),
        nul(c.get(2)),
        nul(c.get(3)),
        nul(c.get(11)));
  }

  static String nul(String s) { // OFAC writes nulls as -0-
    s = s.trim();
    return "-0-".equals(s) ? null : s;
  }
}
