#
# Copyright © 2024 DataSQRL (contact@datasqrl.com)
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

RUN mkdir -p /opt/flink/plugins/flink-jar-runner
COPY flink-jar-runner.uber.jar /opt/flink/plugins/flink-jar-runner
COPY system-functions-discovery-${project.version}.jar /opt/flink/lib/system-functions-discovery.jar
