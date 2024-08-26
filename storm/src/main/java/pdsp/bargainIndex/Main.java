package pdsp.bargainIndex;

import pdsp.config.Config;
import pdsp.config.RandomParallelismEnumerator;

public class Main {
    public static void main(String[] args) {
        String configFilePath = "storm/src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Parameters for the topology
        String topologyName = "bargainIndex"; // Select the BargainIndex topology
        int durationSeconds = config.getIntProperty("durationSeconds");
        double threshold = config.getDoubleProperty("bargainIndex.threshold");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

        // Create and start the topology
        BargainIndexTopology topology = new BargainIndexTopology(topologyName, mode, filePath, kafkaTopic, threshold);
        topology.setDebug(true);
        topology.startTopology(durationSeconds);
    }
}

