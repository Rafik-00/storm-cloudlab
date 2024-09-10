package pdsp.logsAnalytics;


import pdsp.config.Config;

public class Main {
public static void main(String[] args) {
        String configFilePath = "src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Select the topology to run
        String topologyName = "logsAnalytics";
        int durationSeconds = config.getIntProperty("durationSeconds");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

        // Create and start the topology
        logsAnalyticsTopology topology = new logsAnalyticsTopology(topologyName, mode, filePath, kafkaTopic, config);
        topology.setDebug(true);
        // topology.startTopology(durationSeconds);
        topology.submitTopology();
    }

}
