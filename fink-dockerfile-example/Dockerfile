FROM gradle:8.6-jdk11 AS build

WORKDIR /app

COPY build.gradle /app/build.gradle

RUN gradle --no-daemon --console=plain copyDependencies

FROM flink:1.19.1-scala_2.12-java11

COPY --from=build /app/build/dependencies /opt/flink/lib

