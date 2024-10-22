# An example upstream docker image

This docker image extends flink's image to add connectors and some base SQRL jars.

https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#starting-a-session-cluster-on-docker

## Submitting a job:
Download flink so we can use it as a submitter:
https://flink.apache.org/downloads/#apache-flink-1191

./flink run /path/to/flink-jar-runner-1.0.0-SNAPSHOT.jar --planfile /path/to/compiled-plan.json