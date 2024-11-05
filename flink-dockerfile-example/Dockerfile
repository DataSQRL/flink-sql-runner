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

FROM gradle:8.6-jdk11 AS build

ARG SQRL_VERSION="0.5.7"

WORKDIR /app

COPY build.gradle /app/build.gradle

ENV SQRL_VERSION=${SQRL_VERSION}

RUN gradle --no-daemon --console=plain copyDependencies

RUN ls -lha /app/build/dependencies

FROM flink:1.19.1-scala_2.12-java11

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

USER 9999

COPY --from=build /app/build/dependencies /opt/flink/lib
