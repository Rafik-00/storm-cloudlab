package pdsp.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

@Deprecated
public class CustomKafkaSpout {
    private KafkaSpoutConfig<String, String> kafkaSpoutConfig;
    private KafkaSpout<String, String> kafkaSpout;

    public CustomKafkaSpout(String consumerGroupId, String topic) {
        kafkaSpoutConfig = KafkaSpoutConfig.builder("localhost:9092", topic)
                // If multiple instances of the same spout are running, they can share the same group ID, allowing Kafka to distribute messages across them evenly
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
                // specifies the class used to deserialize the key of the messages consumed from Kafka. In this example, StringDeserializer is used to convert byte arrays into strings
                .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                // specifies the class used to deserialize the value of the messages consumed from Kafka.
                .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                .build();
        kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);
    }

    public KafkaSpout<String, String> getKafkaSpout() {
        return kafkaSpout;
    }
}
