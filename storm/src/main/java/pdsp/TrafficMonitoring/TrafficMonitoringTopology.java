package pdsp.TrafficMonitoring;
import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;

public class TrafficMonitoringTopology extends AbstractTopology {
    public TrafficMonitoringTopology(String topologyName, String mode, String filePath, String kafkaTopic) {
        super(topologyName, mode, filePath, kafkaTopic);
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        builder.setSpout("fileSpout", spout);
        builder.setBolt("ParserBolt", new ParserBolt(), parallelism).shuffleGrouping("fileSpout");
        builder.setBolt("MapMatcherBolt", new MapMatcherBolt(), parallelism).shuffleGrouping("ParserBolt");
        builder.setBolt("AvgSpeedCalculatorBolt", new AvgSpeedCalculatorBolt(), parallelism).shuffleGrouping("MapMatcherBolt");
        builder.setBolt("LoggerBolt", new LoggerBolt(),1).shuffleGrouping("AvgSpeedCalculatorBolt");
    }
}

/*public static void main(String[] args) {
        // Create a new topology
        TopologyBuilder builder = new TopologyBuilder();

        // Set the spout
        builder.setSpout("fileSpout", new FileSpout("storm/src/main/java/pdsp/TrafficMonitoring/TmTestData.txt"));

        // Set the bolt
        builder.setBolt("ParserBolt", new ParserBolt()).shuffleGrouping("fileSpout");
        builder.setBolt("MapMatcherBolt", new MapMatcherBolt()).shuffleGrouping("ParserBolt");
        builder.setBolt("AvgSpeedCalculatorBolt", new AvgSpeedCalculatorBolt()).shuffleGrouping("MapMatcherBolt");

        // Set the configuration
        Config config = new Config();
        config.registerSerialization(TrafficEvent.class);
        config.registerSerialization(Road.class);
        config.setNumWorkers(2);

        // Create a local cluster
        LocalCluster Cluster = new LocalCluster();
        try {
            Cluster.submitTopology("TMTopology", config, builder.createTopology());
            Thread.sleep(50000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            Cluster.shutdown();
        }



    }*/
