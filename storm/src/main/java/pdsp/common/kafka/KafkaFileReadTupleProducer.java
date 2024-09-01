package pdsp.common.kafka;

import pdsp.utils.CustomFileReader;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaFileReadTupleProducer extends KafkaTupleProducer{
    private final CustomFileReader customFileReader;

    /**
     * Creates DataTuples on a given kafka topic
     *
     * @param queryName  Query to create topics for.
     * @param props      KafkaConfigs for the producers
     */
    public KafkaFileReadTupleProducer(String queryName, String topic, Properties props, String filePath){
        super(queryName, topic, props);
        customFileReader = new CustomFileReader(filePath);
        customFileReader.open();
    }

    @Override
    public String nextTuple() {
        ArrayList<Object> tuple = readNext();
        // Add the start timestamp
        tuple.add(System.currentTimeMillis());
        return tuple.toString();
    }

    public ArrayList<Object> readNext() {
        String line = customFileReader.readNextTuple();
        if(line == null){
            customFileReader.resetFile();
            line = customFileReader.readNextTuple();
        }
        ArrayList<Object> tupleValues = new ArrayList<>();
        tupleValues.add(line);
        return tupleValues;
    }
}
