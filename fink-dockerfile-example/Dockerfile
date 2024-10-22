FROM gradle:8.6-jdk11 AS build

WORKDIR /app

COPY build.gradle /app/build.gradle

RUN gradle --no-daemon --console=plain copyDependencies

FROM flink:1.19.1-scala_2.12-java11

RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop
RUN ln -fs /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/.

COPY --from=build /app/build/dependencies /opt/flink/lib

