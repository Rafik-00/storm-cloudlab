package pdsp.common;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;

import java.util.Collections;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.NimbusClient;
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
        //this.cluster = new LocalCluster();
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
        //this.cluster = new LocalCluster();
        this.submitter = new StormSubmitter();
        this.config = new Config();
        this.config.put("kafka.bootstrap.server", config.getProperty("kafka.bootstrap.server"));
        this.config.put("kafka.port", config.getProperty("kafka.port"));
        this.config.put("storm.cluster.mode", "distributed");
        this.config.put("storm.local.dir", "/tmp/storm");
        this.config.put("nimbus.thrift.port", 6627);
        this.config.put("storm.zookeeper.port", 2181);
        this.config.put("topology.builtin.metrics.bucket.size.secs", 10);
        this.config.put("drpc.request.timeout.secs", 1600);
        this.config.put("supervisor.worker.start.timeout.secs", 1200);
        this.config.put("nimbus.supervisor.timeout.secs", 1200);
        this.config.put("nimbus.task.launch.secs", 1200);
        this.config.put("storm.zookeeper.servers", Collections.singletonList(config.getProperty("storm.zookeeper.servers")));
        this.config.put("nimbus.seeds", Collections.singletonList(config.getProperty("nimbus.seeds")));
        this.config.put("storm.thrift.socket.timeout.ms", 60000);
        this.config.put("storm.thrift.max_buffer_size", 1048576);
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
    public void submitTopology(int durationSeconds) {
        try {
            buildTopology();
            config.put("topology.queryName", topologyName);
            config.put("topology.parallelismHint", parallelism);
            config.put(config.NIMBUS_SEEDS, Collections.singletonList("localhost"));
            
            NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config);
            Nimbus.Client client = nimbusClient.getClient();

            KafkaRunner runner = new KafkaRunner(config);
            runner.start(topologyName, kafkaTopic, filePath);

            submitter.submitTopology(topologyName, config, builder.createTopology());
            LOG.info("Topology {} started", topologyName);
            Thread.sleep(durationSeconds * 1000);

            runner.stop();
            client.killTopology(topologyName);
            LOG.info("Topology {} stopped", topologyName);
        } catch (TTransportException e) {
            LOG.error("Transport error while running the topology", e);
        } catch (TException e) {
            LOG.error("Thrift error while running the topology", e);
        } catch (Exception e) {
            LOG.error("Error while running the topology", e);
        }
    }
    public void executeSequentialOnRemoteCluster(int durationSeconds) throws Exception {
        buildTopology();
            config.put("topology.queryName", topologyName);
            config.put("topology.parallelismHint", parallelism);
        try {
        // Create the client
        Nimbus.Client client =
                (Nimbus.Client) NimbusClient.getConfiguredClient(config).getClient();

        // Log existing supervisors found in the cluster
        for (Iterator<SupervisorSummary> it = client.getClusterInfo().get_supervisors_iterator(); it.hasNext(); ) {
            LOG.info("Supervisor found: {}", it.next());
        }
        // add local host mapping to the config
        //addLocalHostMappingToConfig(stormConfig, client);

        if (client.getClusterInfo().get_topologies_size() != 0) {
            throw new RuntimeException("There are already queries running on the cluster. Aborting");
        }            
            KafkaRunner kafkaRunner = new KafkaRunner(config);
            kafkaRunner.start(topologyName, kafkaTopic, filePath);
        
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
            Thread.sleep(durationSeconds * 1000);
            kafkaRunner.stop();
            LOG.info("Killing query: {}", topologyName);
            client.killTopology(topologyName);
            Thread.sleep(durationSeconds * 1000);
            LOG.info("Killing query: {}", topologyName);
    }
    catch (Exception e) {
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
