package pdsp.common;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pdsp.utils.JsonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoggerBolt implements IRichBolt {
    private final ArrayList<Long> arrivalTimestamps;
    private final ArrayList<Long> e2eLatencies;
    private final ArrayList<Long> processingLatencies;
    private long startTime;
    private long lastEntryTime;
    private String queryName;
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    private int taskId;
    private int inputCounter;
    private long parallelismHint;

    public LoggerBolt() {
        this.arrivalTimestamps = new ArrayList<>();
        this.e2eLatencies = new ArrayList<>();
        this.processingLatencies = new ArrayList<>();
        this.startTime = 0;
        this.lastEntryTime = 0;
        this.inputCounter = 0;
        this.parallelismHint = 1;
    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.queryName = (String) map.get("topology.queryName");
        this.taskId = topologyContext.getThisTaskId();
        this.parallelismHint = (long) map.get("topology.parallelismHint");
    }

    /**
     * Initialize a start time after first tuple arrives to later calculate the event rate
     *
     * @param tuple The input tuple to be processed.
     */
    @Override
    public void execute(Tuple tuple) {
        lastEntryTime = System.currentTimeMillis();
        if (startTime == 0) {
            startTime = lastEntryTime;
        }
        inputCounter++;
        arrivalTimestamps.add(this.lastEntryTime);

        logger.info(tuple.getValues().toString());
        // End-to-end latency: time from tuple creation to arrival at the logger bolt
        long e2eTimestamp = (long) tuple.getValueByField("e2eTimestamp");
        e2eLatencies.add(this.lastEntryTime-e2eTimestamp);

        // Processing latency: time from tuple creation to tuple processing
        long processingTimestamp = (long) tuple.getValueByField("processingTimestamp");
        processingLatencies.add(this.lastEntryTime - processingTimestamp);
        logger.info("LoggerBolt-" + taskId + " received tuple " + inputCounter);
    }

    @Override
    public void cleanup() {
        long elapsedTime = lastEntryTime - startTime;
        double seconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
        logger.info("Cleanup called of LoggerBolt-{} for query {}", queryName, taskId);
        logger.info("{} number of tuples arrived at LoggerBolt-{} during execution length of {}",
                this.inputCounter, this.taskId, seconds);

        Map<String, Object> label = new HashMap<>();
        if (this.inputCounter == 0) {
            label.put(Constants.QueryLabels.THROUGHPUT, "null");
            label.put(Constants.QueryLabels.PROCLATENCY, "null");
            label.put(Constants.QueryLabels.COUNTER, "null");
            label.put(Constants.QueryProperties.DURATION, "null");

        } else {
            long startCompute = System.currentTimeMillis();

            // Throughput: tuples per second
            double tpt = (double) this.inputCounter / ((this.lastEntryTime - this.startTime) / 1000.0);
            label.put(Constants.QueryLabels.TPT_MEAN, tpt);

            // E2E latency: end-to-end latency
            double e2eMean = this.e2eLatencies.stream().mapToDouble(val -> val).average().orElse(0.0);
            label.put(Constants.QueryLabels.E2E_MEAN, e2eMean);

            // Proc-Latency: mean processing latency in ms
            double procMean = this.processingLatencies.stream().mapToDouble(val -> val).average().orElse(0.0);
            label.put(Constants.QueryLabels.PROC_MEAN, procMean);

            label.put(Constants.QueryLabels.COUNTER, this.inputCounter);
            label.put(Constants.QueryProperties.DURATION, seconds);
            label.put(Constants.QueryProperties.PARALLELISM_HINT, parallelismHint);

            logger.info("LoggerBolt-{} required {} ms to compute label", taskId, (System.currentTimeMillis() - startCompute));
        }

        label.put(Constants.QueryProperties.QUERY, this.queryName);
        logger = LoggerFactory.getLogger("labels");
        logger.info(label.toString());

        Document document = new Document();
        document.putAll(label);

        logger.info("LoggerBolt-{}: Logging label {} for query {}", taskId, document.toJson(), this.queryName);
        try {
            JsonUtils.saveJsonToFile(document.toJson(), "tmp/" + queryName + "-" + parallelismHint + ".json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
