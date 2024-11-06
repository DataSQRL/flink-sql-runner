/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flink.client.AccumulatorsResponse;
import com.datasqrl.flink.client.JarUploadResponse;
import com.datasqrl.flink.client.JobDetails;
import com.datasqrl.flink.client.JobMetric;
import com.datasqrl.flink.client.JobRequest;
import com.datasqrl.flink.client.JobSubmitResponse;
import feign.form.FormData;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.Test;

class FlinkMainIT extends AbstractITSupport {

  @Test
  void test() throws IOException {
    File jarFile = new File("target/flink-jar-runner-1.0.0-SNAPSHOT.jar");

    byte[] fileContent = Files.readAllBytes(jarFile.toPath());

    // Create FormData with content type, file name, and data
    FormData formData = new FormData("application/java-archive", jarFile.getName(), fileContent);

    JarUploadResponse uploadResponse = client.uploadJar(formData);

    assertThat(uploadResponse.status()).isEqualToIgnoringCase("success");
    System.out.println("Jar uploaded successfully");

    // Step 2: Extract jarId from the response
    String jarId =
        uploadResponse.filename().substring(uploadResponse.filename().lastIndexOf("/") + 1);

    // Step 3: Submit the job
    JobRequest jobRequest =
        JobRequest.builder()
            .programArgs("--input /path/to/input --output /path/to/output")
            .parallelism(1)
            .build();

    JobSubmitResponse jobResponse = client.submitJob(jarId, jobRequest);
    String jobId = jobResponse.jobid();
    assertThat(jobId).isNotNull();

    JobDetails jobDetails = client.getJobDetails(jobId);
    System.out.println("Job State: " + jobDetails.state());
    // Step 4: Check job details

    // Step 5: Get Job Metrics
    List<JobMetric> jobMetrics = client.getJobMetrics(jobId);
    for (JobMetric metric : jobMetrics) {
      System.out.println("Metric ID: " + metric.id() + ", Value: " + metric.value());
    }

    // Step 6: Get Job Accumulators
    AccumulatorsResponse accumulatorsResponse = client.getJobAccumulators(jobId);
    if (accumulatorsResponse.userTaskAccumulators() != null) {
      for (AccumulatorsResponse.AccumulatorInfo accumulator :
          accumulatorsResponse.userTaskAccumulators()) {
        System.out.printf(
            "Accumulator Name: %s, Type: %s, Value: %s%n",
            accumulator.name(), accumulator.type(), accumulator.value());
      }
    }
  }
}
