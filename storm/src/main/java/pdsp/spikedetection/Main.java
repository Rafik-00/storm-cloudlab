package pdsp.spikedetection;


import pdsp.config.Config;

public class Main {
      public static void main(String[] args) {
            String configFilePath = "storm/src/main/java/pdsp/config/config.properties";
            Config config = new Config(configFilePath);

            // Parameters for the topology
            String topologyName = "spikeDetection"; // Select the SpikeDetection topology
            int durationSeconds = config.getIntProperty("durationSeconds");
            double threshold = config.getDoubleProperty("spikeDetection.threshold");

            // Load specific topology configurations
            String mode = config.getTopologyProperty(topologyName, "mode");
            String filePath = config.getTopologyProperty(topologyName, "filePath");
            String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

            // Create and start the topology
            SpikeDetectionTopology topology = new SpikeDetectionTopology(topologyName ,mode, filePath, kafkaTopic, threshold, config);
            topology.setDebug(true);
            topology.startTopology(durationSeconds);
      }
}

