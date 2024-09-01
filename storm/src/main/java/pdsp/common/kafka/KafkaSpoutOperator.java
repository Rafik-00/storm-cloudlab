package pdsp.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

public class KafkaSpoutOperator {
    final String bootstrapServer;
    private final String topic;
    final int kafkaPort;
    private final Config config;
    private final boolean localExecution;

    public KafkaSpoutOperator(String topic, String bootstrapServer, int kafkaPort, Config config) {
        this.topic = topic;
        this.bootstrapServer = bootstrapServer;
        this.kafkaPort = kafkaPort;
        this.config = config;

        // TODO: Set localExecution based on config
//        this.localExecution = config.get("storm.cluster.mode").equals("local");
        this.localExecution = true;
    }

    public KafkaSpout<?, ?> getSpoutOperator(String queryName) {
        KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder(bootstrapServer + ":" + kafkaPort, topic);
        builder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST);  // setting the offset strategy to latest --> always start at the end offset of a stream
        builder.setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.NO_GUARANTEE);
        builder.setProp(ConsumerConfig.GROUP_ID_CONFIG, this.topic + "-consumer");

        // specifies the class used to deserialize the key of the messages consumed from Kafka. In this example, StringDeserializer is used to convert byte arrays into strings
        builder.setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // specifies the class used to deserialize the value of the messages consumed from Kafka.
        builder.setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaSpout<>(builder.build());
    }
}
