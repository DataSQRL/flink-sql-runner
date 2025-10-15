#
# Copyright © 2025 DataSQRL (contact@datasqrl.com)
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

FROM flink:${flink-base-image}

RUN mkdir -p /opt/flink/plugins/flink-sql-runner

COPY flink-s3-fs-hadoop-*.jar /opt/flink/lib
COPY hadoop-hdfs-client-*.jar /opt/flink/lib
COPY iceberg-flink-runtime-*.jar /opt/flink/lib
COPY aws-sdk/*.jar /opt/flink/lib/
COPY stdlib-utils-*.jar /opt/flink/lib
COPY flink-sql-runner.uber.jar /opt/flink/plugins/flink-sql-runner
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh \
  && ln -s /opt/flink/plugins/flink-sql-runner/flink-sql-runner.uber.jar /opt/flink/flink-sql-runner.jar

ENTRYPOINT ["/entrypoint.sh"]
