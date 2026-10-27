import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Writes partition 0 of a topic for {@code KafkaSourceWatermarkIT}, which runs it in its Flink
 * container. Two writers commit records in transactions of their own, so both commit markers
 * follow the records, and then one of them writes more records in a transaction it aborts.
 *
 * <p>Arguments: bootstrap servers, topic, records to commit, records to abort.
 */
public class TransactionalProducer {

  public static void main(String[] args) {
    String bootstrapServers = args[0];
    String topic = args[1];
    int committed = Integer.parseInt(args[2]);
    int aborted = Integer.parseInt(args[3]);

    try (var first = writer(bootstrapServers, topic + "-first");
        var second = writer(bootstrapServers, topic + "-second")) {
      first.initTransactions();
      second.initTransactions();

      first.beginTransaction();
      second.beginTransaction();
      for (int id = 0; id < committed; id++) {
        send(id % 2 == 0 ? first : second, topic, id);
      }
      first.flush();
      second.flush();
      first.commitTransaction();
      second.commitTransaction();

      first.beginTransaction();
      for (int id = committed; id < committed + aborted; id++) {
        send(first, topic, id);
      }
      first.flush();
      first.abortTransaction();
    }
  }

  private static KafkaProducer<String, String> writer(
      String bootstrapServers, String transactionalId) {
    var properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("transactional.id", transactionalId);
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", StringSerializer.class.getName());
    return new KafkaProducer<>(properties);
  }

  private static void send(KafkaProducer<String, String> writer, String topic, int id) {
    writer.send(
        new ProducerRecord<>(
            topic, 0, null, "{\"id\":%d,\"payload\":\"record-%d\"}".formatted(id, id)));
  }
}
