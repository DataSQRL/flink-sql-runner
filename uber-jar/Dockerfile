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

FROM maven:3.9.9-eclipse-temurin-11 AS build

ARG RUNNER_VERSION="1.0.0-SNAPSHOT"
ARG SQRL_VERSION="0.5.7"
ARG GITHUB_ACTOR
ARG GITHUB_TOKEN

ENV GITHUB_ACTOR=${GITHUB_ACTOR}
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

ENV SQRL_VERSION=${SQRL_VERSION}

WORKDIR /src
COPY * /src/
RUN mvn clean install -B -Pquickbuild

WORKDIR /uber-jar
COPY uber-jar/uber-pom.xml /uber-jar/pom.xml
RUN mvn clean install -B

FROM alpine:3

ARG RUNNER_VERSION="1.0.0-SNAPSHOT"
ENV RUNNER_VERSION=${RUNNER_VERSION}

USER 9999

WORKDIR /runner

COPY --from=build /uber-jar/target/flink-jar-runner-uber-jar-${RUNNER_VERSION}.jar /runner/
