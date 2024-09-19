package pdsp.sentimentAnalysis;

import pdsp.config.Config;

public class Main {
    public static void main(String[] args) throws Exception{
        String configFilePath = "src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Select the topology to run
        String topologyName = "sentimentAnalysis";
        int durationSeconds = config.getIntProperty("durationSeconds");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

        // Create and start the topology
        SentimentAnalysisTopology topology = new SentimentAnalysisTopology(topologyName, mode, filePath, kafkaTopic, config);
        topology.setDebug(true);
        topology.startTopology(durationSeconds);
    }
}
