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
import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.camel.FixedWidth;
import com.datasqrl.flinkrunner.camel.Types;
import java.math.BigDecimal;
import java.time.LocalDate;
import org.apache.camel.Exchange;

/** Mainframe GL postings, EBCDIC fixed-width deltas over SFTP, in file mode. */
@CamelSource
public class GlPostings implements FileSource<GlPostings.Posting, String> {

  public record Posting(String postingId, String account, BigDecimal amount, LocalDate postDate) {}

  public String location() {
    return "sftp://erp.example.com/outbound/gl"
        + "?username={{env:ERP_USER}}&privateKeyFile={{env:ERP_KEY}}"
        + "&include=GL_.*\\.dat";
  }

  public Iterable<String> records(Exchange file) {
    return FixedWidth.lines(file, "IBM037"); // EBCDIC
  }

  public Posting map(String line) {
    if (line.startsWith("HDR") || line.startsWith("TRL")) return null;
    return new Posting(
        line.substring(0, 12).trim(),
        line.substring(12, 22).trim(),
        Types.decimal(line.substring(22, 37).trim(), 18, 2),
        Types.date(line.substring(37, 45), "yyyyMMdd"));
  }
}
