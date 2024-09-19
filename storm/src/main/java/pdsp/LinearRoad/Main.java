package pdsp.LinearRoad;

import pdsp.config.Config;


public class Main {

    public static void main(String[] args) throws Exception{


        String configFilePath = "src/main/java/pdsp/config/config.properties";
        Config config = new Config(configFilePath);

        // Parameters for the topology
        String topologyName = "linearRoad"; // Select the LinearRoad topology
        int durationSeconds = config.getIntProperty("durationSeconds");
        int operationSpecification = config.getIntProperty("linearRoad.operationSpecification");

        // Load specific topology configurations
        String mode = config.getTopologyProperty(topologyName, "mode");
        String filePath = config.getTopologyProperty(topologyName, "filePath");
        String kafkaTopic = config.getTopologyProperty(topologyName, "kafkaTopic");

        // Create and start the topology
        LinearRoadTopology topology = new LinearRoadTopology(topologyName, mode, filePath, kafkaTopic, operationSpecification,config );
        topology.setDebug(true);
        topology.startTopology(durationSeconds);


    }

}
