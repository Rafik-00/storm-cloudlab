package pdsp.logsAnalytics;

import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;


public class logsAnalyticsTopology extends AbstractTopology {
    public logsAnalyticsTopology(String topologyName, String mode, String filePath, String kafkaTopic, Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);
    }


    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int logParserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int statusCounterParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int volumeCounterParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((logParserParallelism + statusCounterParallelism + volumeCounterParallelism) / 3.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        builder.setSpout("fileSpout", spout);
        builder.setBolt("logParserBolt", new logParserBolt(), logParserParallelism).shuffleGrouping("fileSpout");
        builder.setBolt("statusCounterBolt", new statusCounterBolt(), statusCounterParallelism).shuffleGrouping("logParserBolt");
        builder.setBolt("volumeCounterBolt", new volumeCounterBolt(), volumeCounterParallelism).shuffleGrouping("logParserBolt");
        builder.setBolt("loggerBolt", new LoggerBolt(), 1).shuffleGrouping("volumeCounterBolt").shuffleGrouping("statusCounterBolt");

    }
}
