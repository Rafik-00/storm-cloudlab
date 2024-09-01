package pdsp.common;

public class Constants {
    interface QueryLabels {
        String PRODUCER_INTERVAL = "producer-interval";
        String INGESTION_INTERVAL = "ingestion-interval";
        String THROUGHPUT = "throughput";
        String TPT_MEAN = "throughput-mean";
        String E2E_MEAN = "e2e-mean";
        String PROC_MEAN = "proc-mean";
        String PROCLATENCY = "proc-latency";
        String COUNTER = "counter";
    }

    interface QueryProperties {
        String DURATION = "duration";
        String QUERY = "query";
        String PARALLELISM_HINT = "parallelism-hint";
    }

    public interface OperatorProperties {
        String OPERATOR_TYPE = "operatorType";
        String COMPONENT = "component";

        String TOPIC = "topic";
        String CONSUMER_POSITION = "consumer";
        String PRODUCER_POSITION = "producer";
        String OFFSET = "offset";
        String KAFKA_DURATION = "kafkaDuration";
    }

    public interface Operators {
        String SPOUT = "spout";
        String AGGREGATE = "aggregation";
        String WINDOW_AGGREGATE = "windowedAggregation";
        String PAIR_AGGREGATE = "pairAggregation";
        String PAIR_ITERABLE_AGGREGATE = "pairIterableAggregation";
        String MAP_TO_PAIR = "mapToPair";
        String WINDOW = "window";
        String FILTER = "filter";
        String PAIR_FILTER = "pairFilter";
        String JOIN = "join";
        String SINK = "sink";
        String HOST = "host";
    }

    public interface Kafka {
        int NUM_THREADS = 5;
        String TOPIC0 = "source0";
        String TOPIC1 = "source1";
        String TOPIC2 = "source2";
        String[] TOPICS = {TOPIC0, TOPIC1, TOPIC2};
    }

    public interface MongoConstants {
        String MONGO_ADDRESS = "mongo.address";
        String MONGO_PORT = "mongo.port";
    }

    public interface Mongo {
        String MONGO_DATABASE = "dsps";
        String MONGO_USER = "dsps_mongo";
        String MONGO_PASSWORD = "s3cure!";
    }
}