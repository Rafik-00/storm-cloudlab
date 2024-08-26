package pdsp.common;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pdsp.config.RandomParallelismEnumerator;

public abstract class AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopology.class);

    protected String topologyName;

    protected TopologyBuilder builder;
    protected Config config;
    protected LocalCluster cluster;
    protected String mode;
    protected String filePath;
    protected String kafkaTopic;
    protected int parallelism;
    protected int windowSize;
    protected int slidingInterval;
    protected RandomParallelismEnumerator parallelismEnumerator = new RandomParallelismEnumerator();

    public AbstractTopology(String topologyName, String mode, String filePath, String kafkaTopic) {
        this.topologyName = topologyName;
        this.mode = mode;
        this.filePath = filePath;
        this.kafkaTopic = kafkaTopic;
        this.windowSize = 2; //TODO: set with Enumerator
        this.slidingInterval = 1; //TODO: set with Enumerator
        this.parallelism = parallelismEnumerator.getRandomParallelismHint();
        this.builder = new TopologyBuilder();
        this.config = new Config();
        this.cluster = new LocalCluster();
    }

    protected abstract void buildTopology();

    protected BaseRichSpout getSpout() {
        if (mode.equalsIgnoreCase("kafka")) {
            return new CustomKafkaSpout("storm-kafka-group", kafkaTopic).getKafkaSpout();
        } else if (mode.equalsIgnoreCase("file")) {
            return new FileSpout(filePath);
        } else {
            throw new IllegalArgumentException("Unsupported mode: " + mode);
        }
    }

    public void startTopology(int durationSeconds) {
        try {
            buildTopology();
            config.put("topology.queryName", topologyName);
            config.put("topology.parallelismHint", parallelism);

            cluster.submitTopology(topologyName, config, builder.createTopology());
            LOG.info("Topology {} started", topologyName);

            Thread.sleep(durationSeconds * 1000);

            cluster.shutdown();
            LOG.info("Topology {} stopped", topologyName);
        } catch (Exception e) {
            LOG.error("Error while running the topology", e);
        }
    }


    public void setDebug(boolean debug) {
        config.setDebug(debug);
    }

    public void registerSerialization(Class<?> clazz) {
        config.registerSerialization(clazz);
    }
}
