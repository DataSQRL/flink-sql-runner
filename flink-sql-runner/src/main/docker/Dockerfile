#
# Copyright © 2026 DataSQRL (contact@datasqrl.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM apache/flink:${flink-base-image}

ARG ASYNC_PROFILER_VERSION=4.4
ARG TARGETARCH

USER root

# Install JDK 17 (replacing the default JRE)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && ln -sf /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# Download and setup async-profiler
RUN set -eu; \
    case "${TARGETARCH:-$(uname -m)}" in \
      x86_64) ASYNC_PROFILER_ARCH="x64" ;; \
      amd64) ASYNC_PROFILER_ARCH="x64" ;; \
      aarch64) ASYNC_PROFILER_ARCH="arm64" ;; \
      arm64) ASYNC_PROFILER_ARCH="arm64" ;; \
      *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-linux-${ASYNC_PROFILER_ARCH}.tar.gz" -o /tmp/async-profiler.tar.gz \
    && tar -xzf /tmp/async-profiler.tar.gz -C /opt \
    && mv "/opt/async-profiler-${ASYNC_PROFILER_VERSION}-linux-${ASYNC_PROFILER_ARCH}" /opt/async-profiler \
    && ln -s /opt/async-profiler/bin/asprof /usr/local/bin/asprof \
    && rm -f /tmp/async-profiler.tar.gz \
    && rm -rf /var/lib/apt/lists/*

COPY flink-s3-fs-hadoop-*.jar /opt/flink/lib
COPY hadoop-common-*.jar /opt/flink/lib
COPY hadoop-hdfs-client-*.jar /opt/flink/lib
COPY hadoop-mapreduce-client-core-*.jar /opt/flink/lib
COPY iceberg-flink-runtime-*.jar /opt/flink/lib
COPY iceberg-aws-bundle-*.jar /opt/flink/lib
COPY stdlib-utils-*.jar /opt/flink/lib
COPY flink-sql-runner.uber.jar /opt/flink/lib/sql-runner.uber.jar
COPY --chmod=755 sql-runner /opt/flink/bin/sql-runner
COPY --chmod=755 entrypoint.sh /entrypoint.sh

RUN echo 'UEsDBBQACAgIAG2UllwAAAAAAAAAAAAAAAAUAAQATUVUQS1JTkYvTUFOSUZFU1QuTUb+ygAA803My0xLLS7RDUstKs7Mz7NSMNQz4HIuSk0sSU3Rdaq0UkjLyczL1i0uzNEtKs3LSy3i4gIAUEsHCL/cUjU2AAAANAAAAFBLAQIUABQACAgIAG2Ully/3FI1NgAAADQAAAAUAAQAAAAAAAAAAAAAAAAAAABNRVRBLUlORi9NQU5JRkVTVC5NRv7KAABQSwUGAAAAAAEAAQBGAAAAfAAAAAAA' | base64 -d > /opt/flink/noop.jar \
    && mkdir -p /opt/flink/hadoop-conf \
    && echo '<configuration><property><name>fs.s3a.aws.credentials.provider</name><value>com.amazonaws.auth.DefaultAWSCredentialsProviderChain</value></property></configuration>' > /opt/flink/hadoop-conf/core-site.xml \
    && chown -R flink:flink /opt/flink

USER flink

ENV HADOOP_CONF_DIR="/opt/flink/hadoop-conf"

ENTRYPOINT ["/entrypoint.sh"]
