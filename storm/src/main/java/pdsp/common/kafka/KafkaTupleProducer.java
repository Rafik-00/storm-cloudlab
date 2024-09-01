package pdsp.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KafkaTupleProducer implements Runnable{
    final Thread thread;
    private final KafkaProducer<byte[], byte[]> producer;
    protected final String queryName;
    private final String topic;
    private final AtomicBoolean terminated;
    private final Logger logger = LoggerFactory.getLogger("kafka");

    public KafkaTupleProducer(String queryName, String topic, Properties props) {
        this.queryName = queryName;
        this.producer = new KafkaProducer<>(props);
        this.thread = new Thread(this, queryName);
        this.terminated = new AtomicBoolean(false);
        this.topic = topic;
    }

    @Override
    public void run() {
        ProducerRecord<byte[], byte[]> record;
        while(!terminated.get()){
            String nextTuple = nextTuple();
            record = new ProducerRecord<>(topic, nextTuple.getBytes());
            producer.send(record);
        }
    }

    public void terminate() {
        terminated.set(true);
    }

    /**
     * returns the next tuple to be sent from Kafka producer.
     * It is expected to contain the start timestamp
     * @return
     */
    public abstract String nextTuple();
}
