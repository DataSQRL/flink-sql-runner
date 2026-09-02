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
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;

/**
 * Staged Kafka connectivity probe, run via the JDK single-file source launcher against the
 * connector classes already on the image classpath.
 *
 * <p>The Flink enumerator reports every pre-metadata failure as the same opaque {@code Timed out
 * waiting for a node assignment. Call: listNodes}. This separates that into DNS, TCP, and
 * TLS/SASL/metadata so the failing layer is named rather than inferred.
 */
public final class KafkaProbe {

  private static final int TCP_TIMEOUT_MS = 5_000;
  private static final int ADMIN_TIMEOUT_MS = 15_000;

  private KafkaProbe() {}

  public static void main(String[] args) {
    var bootstrap = arg(args, 0, env("KAFKA_BOOTSTRAP_SERVERS", ""));
    var topic = arg(args, 1, env("KAFKA_PROBE_TOPIC", ""));

    if (bootstrap.isBlank()) {
      System.out.println(
          "usage: kafka-probe [bootstrap.servers] [topic]   (defaults: $KAFKA_BOOTSTRAP_SERVERS, $KAFKA_PROBE_TOPIC)");
      System.out.println("FAIL [config] bootstrap.servers is empty");
      System.exit(2);
    }

    var protocol = env("SQRL_KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
    var mechanism = env("SQRL_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512");
    var user = env("SQRL_KAFKA_SASL_USERNAME", "");
    var pass = env("SQRL_KAFKA_SASL_PASSWORD", "");

    System.out.println("bootstrap.servers = " + bootstrap);
    System.out.println("security.protocol = " + protocol);
    System.out.println("sasl.mechanism    = " + mechanism);
    System.out.println("sasl username     = " + (user.isBlank() ? "<EMPTY>" : user));
    System.out.println(
        "sasl password     = " + (pass.isBlank() ? "<EMPTY>" : "<set, " + pass.length() + " chars>"));
    System.out.println();

    var brokers = parse(bootstrap);
    var dnsOk = stageDns(brokers);
    var tcpOk = dnsOk && stageTcp(brokers);

    if (!dnsOk) {
      verdict("DNS", "broker hostnames do not resolve - check CoreDNS, the VPC resolver and any split-horizon zone");
      System.exit(1);
    }
    if (!tcpOk) {
      verdict("TCP", "hostnames resolve but no broker port accepts a connection - check security groups, NACLs, routing and VPC peering");
      System.exit(1);
    }
    stageAdmin(bootstrap, protocol, mechanism, user, pass, topic);
  }

  private static boolean stageDns(List<Broker> brokers) {
    System.out.println("== stage 1: DNS ==");
    var ok = false;
    for (var b : brokers) {
      try {
        var addrs = InetAddress.getAllByName(b.host());
        var ips = new ArrayList<String>();
        for (var a : addrs) {
          ips.add(a.getHostAddress());
        }
        b.addresses().addAll(ips);
        System.out.printf("  OK   %s -> %s%n", b.host(), String.join(", ", ips));
        ok = true;
      } catch (UnknownHostException e) {
        System.out.printf("  FAIL %s -> unresolvable%n", b.host());
      }
    }
    System.out.println();
    return ok;
  }

  private static boolean stageTcp(List<Broker> brokers) {
    System.out.println("== stage 2: TCP ==");
    var ok = false;
    for (var b : brokers) {
      for (var ip : b.addresses()) {
        var started = System.nanoTime();
        try (var socket = new Socket()) {
          socket.connect(new InetSocketAddress(ip, b.port()), TCP_TIMEOUT_MS);
          System.out.printf("  OK   %s:%d open (%d ms)%n", ip, b.port(), elapsedMs(started));
          ok = true;
        } catch (SocketTimeoutException e) {
          System.out.printf(
              "  FAIL %s:%d timed out after %d ms - packets dropped, typically a security group or NACL%n",
              ip, b.port(), elapsedMs(started));
        } catch (ConnectException e) {
          System.out.printf(
              "  FAIL %s:%d refused - reachable but nothing is listening on that port%n", ip, b.port());
        } catch (IOException e) {
          System.out.printf("  FAIL %s:%d %s%n", ip, b.port(), e);
        }
      }
    }
    System.out.println();
    return ok;
  }

  private static void stageAdmin(
      String bootstrap, String protocol, String mechanism, String user, String pass, String topic) {
    System.out.println("== stage 3: TLS + SASL + metadata ==");

    var props = new Properties();
    props.put("bootstrap.servers", bootstrap);
    props.put("security.protocol", protocol);
    props.put("request.timeout.ms", String.valueOf(ADMIN_TIMEOUT_MS));
    props.put("default.api.timeout.ms", String.valueOf(ADMIN_TIMEOUT_MS));
    if (protocol.startsWith("SASL")) {
      props.put("sasl.mechanism", mechanism);
      props.put(
          "sasl.jaas.config",
          "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
              + user
              + "\" password=\""
              + pass
              + "\";");
    }

    var timeout = new DescribeClusterOptions().timeoutMs(ADMIN_TIMEOUT_MS);
    try (var admin = Admin.create(props)) {
      var nodes = admin.describeCluster(timeout).nodes().get();
      System.out.println("  OK   describeCluster -> " + nodes);

      if (!topic.isBlank()) {
        var described =
            admin
                .describeTopics(List.of(topic), new DescribeTopicsOptions().timeoutMs(ADMIN_TIMEOUT_MS))
                .allTopicNames()
                .get();
        var partitions = described.get(topic).partitions().size();
        System.out.printf("  OK   describeTopics -> %s (%d partitions)%n", topic, partitions);
      }
      System.out.println();
      verdict("REACHABLE", "the connector configuration in this pod can reach Kafka");
    } catch (Exception e) {
      var root = rootCause(e);
      var name = root.getClass().getSimpleName();
      System.out.printf("  FAIL %s: %s%n%n", root.getClass().getName(), root.getMessage());
      verdict(name, explain(name));
      System.exit(1);
    }
  }

  private static String explain(String exception) {
    return switch (exception) {
      case "TimeoutException" ->
          "TCP opened but no broker completed a handshake - usually TLS interception or a broker refusing the listener";
      case "SslAuthenticationException", "SSLHandshakeException", "SSLException" ->
          "TLS failed - wrong port for the listener, or an untrusted certificate chain";
      case "SaslAuthenticationException" -> "credentials rejected - check the SASL username and password";
      case "TopicAuthorizationException" -> "authenticated, but the principal lacks ACLs on that topic";
      case "GroupAuthorizationException" -> "authenticated, but the principal lacks ACLs on the consumer group";
      case "UnknownTopicOrPartitionException" -> "authenticated and authorized, but the topic does not exist";
      case "ConfigException" -> "client configuration rejected before any connection was attempted";
      default -> "see the exception above";
    };
  }

  private static void verdict(String label, String detail) {
    System.out.println("VERDICT: " + label + " - " + detail);
  }

  private static List<Broker> parse(String bootstrap) {
    var brokers = new ArrayList<Broker>();
    for (var entry : bootstrap.split(",")) {
      var trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      var sep = trimmed.lastIndexOf(':');
      if (sep < 0) {
        brokers.add(new Broker(trimmed, 9092, new ArrayList<>()));
      } else {
        brokers.add(
            new Broker(
                trimmed.substring(0, sep),
                Integer.parseInt(trimmed.substring(sep + 1)),
                new ArrayList<>()));
      }
    }
    return brokers;
  }

  private static Throwable rootCause(Throwable t) {
    var root = t;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return root;
  }

  private static long elapsedMs(long startedNanos) {
    return Duration.ofNanos(System.nanoTime() - startedNanos).toMillis();
  }

  private static String arg(String[] args, int index, String fallback) {
    return args.length > index ? args[index] : fallback;
  }

  private static String env(String name, String fallback) {
    var value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }

  private record Broker(String host, int port, List<String> addresses) {}
}
