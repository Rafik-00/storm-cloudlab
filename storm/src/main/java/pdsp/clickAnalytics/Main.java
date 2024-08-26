package pdsp.clickAnalytics;

import pdsp.config.Config;

public class Main {
    public static void main(String[] args) {
        String configFilePath = "storm/src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Select the topology to run
        String topologyName = "clickAnalytics";
        int durationSeconds = config.getIntProperty("durationSeconds");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");
        int slidingWindowSize = Integer.parseInt(config.getTopologyProperty(topologyName, "windowSize"));
        int slidingWindowSlide = Integer.parseInt(config.getTopologyProperty(topologyName, "slideSize"));

        // Create and start the topology
        ClickAnalyticsTopology topology = new ClickAnalyticsTopology(topologyName, mode, filePath, kafkaTopic, slidingWindowSize, slidingWindowSlide);
        topology.setDebug(true);
        topology.startTopology(durationSeconds);
    }
}
