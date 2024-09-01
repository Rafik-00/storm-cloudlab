package pdsp.TrafficMonitoring;
import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class TrafficMonitoringTopology extends AbstractTopology {
    public TrafficMonitoringTopology(String topologyName, String mode, String filePath, String kafkaTopic, Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int parserOperatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int mapMatcherParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int avgSpeedCalculatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((parserOperatorParallelism + mapMatcherParallelism + avgSpeedCalculatorParallelism) / 3.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        builder.setSpout("fileSpout", spout);
        builder.setBolt("ParserBolt", new ParserBolt(), parserOperatorParallelism).shuffleGrouping("fileSpout");
        builder.setBolt("MapMatcherBolt", new MapMatcherBolt(), mapMatcherParallelism).shuffleGrouping("ParserBolt");
        builder.setBolt("AvgSpeedCalculatorBolt", new AvgSpeedCalculatorBolt(), avgSpeedCalculatorParallelism).shuffleGrouping("MapMatcherBolt");
        builder.setBolt("LoggerBolt", new LoggerBolt(),1).shuffleGrouping("AvgSpeedCalculatorBolt");
    }
}

/*public static void main(String[] args) {
        // Create a new topology
        TopologyBuilder builder = new TopologyBuilder();

        // Set the spout
        builder.setSpout("fileSpout", new FileSpout("storm/src/main/java/pdsp/TrafficMonitoring/taxi-traces.csv"));

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
