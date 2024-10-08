package pdsp.wordCount;

import pdsp.config.Config;

public class Main {
    public static void main(String[] args) throws Exception {
        String configFilePath = "src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Parameters for the topology
        String topologyName = "wordCount"; // Select the WordCount topology
        int durationSeconds = config.getIntProperty("durationSeconds");
        double threshold = config.getDoubleProperty("spikeDetection.threshold");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

        // Create and start the topology
        WordCountTopology topology = new WordCountTopology(topologyName, mode, filePath, kafkaTopic, config);
        topology.setDebug(false);
        topology.startTopology(durationSeconds);



    }
}
