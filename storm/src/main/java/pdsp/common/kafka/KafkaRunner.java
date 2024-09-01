package pdsp.common.kafka;

import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.storm.Config;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pdsp.common.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class KafkaRunner {
    private final Logger logger = LoggerFactory.getLogger("kafka");
    private ArrayList<KafkaTupleProducer> kafkaTupleProducers;
    private String queryName;
    private final Properties consumerProperties;
    private final Properties producerProperties;
    private final boolean localExecution;
    private MongoCollection<Document> collection;
//    private int numSources;
    private long startTime;
    private final int numOfThreads;
    private String topic;

    public KafkaRunner(Config config) {
        this(config, Constants.Kafka.NUM_THREADS);
    }

    public KafkaRunner(Config config, int numOfThreads) {
        // TODO: Set localExecution based on config
//        this.localExecution = config.get("storm.cluster.mode").equals("local");
        this.localExecution = true;
        this.numOfThreads = numOfThreads;
        this.kafkaTupleProducers = new ArrayList<>();

        // TODO: FIX MONGO DEPENDENCY
//        this.mode = mode;
//        if (!this.localExecution) {
//            MongoClient mongoClient = MongoClientProvider.getMongoClient(config);
//            this.collection = mongoClient
//                    .getDatabase((String) config.get("mongo.database"))
//                    .getCollection((String) config.get("mongo.collection.offsets"));
//        }
//        kafkaTupleProducers = new ArrayList<>();

        producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.server") + ":" + config.get("kafka.port"));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "0");

        consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "dsps");
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("kafka.bootstrap.server") + ":" + config.get("kafka.port"));
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }


    public void start(String queryName, String topic, String filepath) {
        this.queryName = queryName;
        this.topic = topic;
        logger.info("Starting {} KafkaTupleProducer for executing query {}", this.numOfThreads, this.queryName);

        kafkaTupleProducers.clear();

        for (int threadIdx = 0; threadIdx < this.numOfThreads; threadIdx++) {
            // assigning unique Client_ID is afforded
            producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
                    topic + "-" + threadIdx + "-" + this.queryName);

            KafkaTupleProducer producer = new KafkaFileReadTupleProducer(this.queryName, topic, producerProperties, filepath);
            kafkaTupleProducers.add(producer);
            producer.thread.start();
        }
        this.startTime = System.currentTimeMillis();
    }

    public void stop() {
        long endTime = System.currentTimeMillis();

        for (KafkaTupleProducer producer : kafkaTupleProducers) {
            producer.terminate();
        }
        kafkaTupleProducers.clear();

        // TODO: Identify last offset of the query?

        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, this.queryName + "-" + this.topic + "-end-offset-client");
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerProperties);
        TopicPartition partition = new TopicPartition(this.topic, 0);
        consumer.assign(Collections.singletonList(partition));
        consumer.seekToEnd(Collections.singleton(partition));
        long position = consumer.position(partition);
        consumer.close();

        // write offset to disk or database
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("query", this.queryName);
        offset.put(Constants.OperatorProperties.PRODUCER_POSITION, position);
        offset.put(Constants.OperatorProperties.TOPIC, topic);
        offset.put(Constants.OperatorProperties.KAFKA_DURATION, (endTime - startTime));
        Document document = new Document();
        document.putAll(offset);
        if (localExecution) {
            Logger newLogger = LoggerFactory.getLogger("offsets");
            newLogger.info(document.toJson());
        } else {
            collection.insertOne(document);
        }
    }
}
