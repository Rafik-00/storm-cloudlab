package pdsp.common;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pdsp.common.kafka.KafkaRunner;
import pdsp.common.kafka.KafkaSpoutOperator;
import pdsp.config.RandomParallelismEnumerator;

public abstract class AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopology.class);

    protected String topologyName;

    protected TopologyBuilder builder;
    protected Config config;
    protected LocalCluster cluster;
    protected StormSubmitter submitter;
    protected String mode;
    protected String filePath;
    protected String kafkaTopic;
    protected int parallelism;
    protected int windowSize;
    protected int slidingInterval;
    protected RandomParallelismEnumerator parallelismEnumerator = new RandomParallelismEnumerator();

    // TODO: Remove this constructor once all topologies are adjusted kafka spout
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
        this.submitter = new StormSubmitter();
    }

    // Added constructor to pass config
    public AbstractTopology(String topologyName, String mode, String filePath, String kafkaTopic, pdsp.config.Config config) {
        this.topologyName = topologyName;
        this.mode = mode;
        this.filePath = filePath;
        this.kafkaTopic = kafkaTopic;
        this.windowSize = 2; //TODO: set with Enumerator
        this.slidingInterval = 1; //TODO: set with Enumerator
        this.parallelism = parallelismEnumerator.getRandomParallelismHint();
        this.builder = new TopologyBuilder();
        this.cluster = new LocalCluster();

        this.config = new Config();
        this.config.put("kafka.bootstrap.server", config.getProperty("kafka.bootstrap.server"));
        this.config.put("kafka.port", config.getProperty("kafka.port"));
    }

    protected abstract void buildTopology();

    protected BaseRichSpout getSpout() {
        if (mode.equalsIgnoreCase("kafka")) {
            String bootstrapServer = (String) config.get("kafka.bootstrap.server");
            int port = Integer.parseInt((String) config.get("kafka.port"));
            return new KafkaSpoutOperator(kafkaTopic, bootstrapServer, port, config).getSpoutOperator(topologyName);
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

            KafkaRunner runner = new KafkaRunner(config);
            runner.start(topologyName, kafkaTopic, filePath);

            cluster.submitTopology(topologyName, config, builder.createTopology());
            LOG.info("Topology {} started", topologyName);

            Thread.sleep(durationSeconds * 1000);

            runner.stop();
            cluster.shutdown();
            LOG.info("Topology {} stopped", topologyName);
        } catch (Exception e) {
            LOG.error("Error while running the topology", e);
        }
    }
    public void submitTopology() {
        try {
            buildTopology();
            config.put("topology.queryName", topologyName);
            config.put("topology.parallelismHint", parallelism);
            config.put(Config.NIMBUS_SEEDS, "localhost");

            KafkaRunner runner = new KafkaRunner(config);
            runner.start(topologyName, kafkaTopic, filePath);

            submitter.submitTopology(topologyName, config, builder.createTopology());
            LOG.info("Topology {} started", topologyName);

            runner.stop();
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
