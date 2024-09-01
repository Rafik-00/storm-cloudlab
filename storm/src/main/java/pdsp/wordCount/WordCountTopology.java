package pdsp.wordCount;

import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class WordCountTopology extends AbstractTopology {
    public WordCountTopology(String topologyName, String mode, String filePath, String kafkaTopic, Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int splitterParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int wordCountBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((splitterParallelism + wordCountBoltParallelism) / 2.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        // Define the topology
        builder.setSpout("fileSpout", spout);
        builder.setBolt("splitter", new splitter(),splitterParallelism).shuffleGrouping("fileSpout");
        builder.setBolt("wordCountBolt", new WordCountBolt(),wordCountBoltParallelism).shuffleGrouping("splitter");
        builder.setBolt("loggerBolt", new LoggerBolt(), 1).shuffleGrouping("wordCountBolt");


    }
}
