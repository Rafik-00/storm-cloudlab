package pdsp.machineOutlier;

import pdsp.config.Config;

public class Main {
    public static void main(String[] args) {
        String configFilePath = "storm/src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Select the topology to run
        String topologyName = "machineOutlier";
        int durationSeconds = config.getIntProperty("durationSeconds");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");
        int slidingWindowSize = Integer.parseInt(config.getTopologyProperty(topologyName, "windowSize"));
        int slidingWindowSlide = Integer.parseInt(config.getTopologyProperty(topologyName, "slideSize"));
        double threshold = Double.parseDouble(config.getTopologyProperty(topologyName, "threshold"));

        // Create and start the topology
        MachineOutlierTopology topology = new MachineOutlierTopology(topologyName, mode, filePath, kafkaTopic, slidingWindowSize, slidingWindowSlide, threshold, config);
        topology.setDebug(true);
        topology.startTopology(durationSeconds);
    }
}
