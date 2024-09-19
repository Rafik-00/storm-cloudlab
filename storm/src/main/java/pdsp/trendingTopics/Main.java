package pdsp.trendingTopics;

import pdsp.config.Config;

public class Main {


    public static void main(String[] args) throws Exception{
        String configFilePath = "src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Parameters for the topology
        String topologyName = "trendingTopics"; // Select the TrendingTopics topology
        int durationSeconds = config.getIntProperty("durationSeconds");
        int windowDurationSeconds = config.getIntProperty("trendingTopics.windowDurationSeconds");
        int topicPopularityThreshold = config.getIntProperty("trendingTopics.topicPopularityThreshold");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");


        // Create and start the topology
        TrendingTopicsTopology topology = new TrendingTopicsTopology(topologyName ,mode, filePath, kafkaTopic, windowDurationSeconds,topicPopularityThreshold, config);
        topology.setDebug(true);
        topology.startTopology(durationSeconds);


    }
}
