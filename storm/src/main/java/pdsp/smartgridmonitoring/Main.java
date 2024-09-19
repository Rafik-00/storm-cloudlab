package pdsp.smartgridmonitoring;

import pdsp.config.Config;


public class Main {


    public static void main(String[] args) {
        String configFilePath = "storm/src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Parameters for the topology
        String topologyName = "smartGridMonitoring"; // Select the SmartGridMonitoring topology
        int durationSeconds = config.getIntProperty("durationSeconds");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

        // Create and start the topology
        SmartGridMonitoringTopology topology = new SmartGridMonitoringTopology(topologyName, mode, filePath, kafkaTopic,config);
        topology.setDebug(true);
        topology.startTopology(durationSeconds);
    }
}
