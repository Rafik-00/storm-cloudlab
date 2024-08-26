package pdsp.wordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.ComponentConfigurationDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;

import java.util.Scanner;

public class WordCountTopology extends AbstractTopology {
    public WordCountTopology(String topologyName, String mode, String filePath, String kafkaTopic) {
        super(topologyName, mode, filePath, kafkaTopic);
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        // Define the topology
        builder.setSpout("fileSpout", spout);
        builder.setBolt("splitter", new splitter()).shuffleGrouping("fileSpout");
        builder.setBolt("wordCountBolt", new WordCountBolt()).shuffleGrouping("splitter");


    }
    /*public static void main(String[] args) {
        /*
        change the degree of parallelism for spout and bolt form user input
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter degree of parallelism for spout: ");
        int spoutParallelism = scanner.nextInt();
        System.out.println("degree of parallelism chosen for spout: " + spoutParallelism);
        System.out.println("Enter degree of parallelism for bolt: ");
        int boltParallelism = scanner.nextInt();
        System.out.println("degree of parallelism chosen for bolt: " + boltParallelism);

         */
    /*
        System.out.println("Topology is starting...");
        long startTime = System.currentTimeMillis();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("fileSpout", new FileSpout());
        builder.setBolt("wordCountBolt", new WordCountBolt(), 1).shuffleGrouping("fileSpout");


        Config config = new Config();
        config.setDebug(true);

        //set the number of workers to 2
        config.setNumWorkers(2);
        // set the number of tasks to 2
//        ComponentConfigurationDeclarer boltConfig = new ComponentConfigurationDeclarer();
//        boltConfig.setNumTasks(2);

        LocalCluster Cluster = new LocalCluster();
        try {
            Cluster.submitTopology("WordCountTopology", config, builder.createTopology());
            Thread.sleep(10000);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            Cluster.shutdown();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken: " + (endTime - startTime) + " milliseconds");
    }*/
}
