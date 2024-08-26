package pdsp.adAnalytics;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;

import org.apache.storm.tuple.Fields;

public class AdAnalyticsTopology extends AbstractTopology {
    public AdAnalyticsTopology(String topologyName, String mode, String filePath, String kafkaTopic) {
        super(topologyName, mode, filePath, kafkaTopic);
    }
    @Override 
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int parserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int counterParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int rollingCounterParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int)Math.round((parserParallelism + counterParallelism + rollingCounterParallelism) /3.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        builder.setSpout("fileSpout", spout);
        builder.setBolt("ad-event-parser", new AdEventParserBolt(), parserParallelism ).shuffleGrouping("fileSpout");
        builder.setBolt("counter", new AdEventCounterBolt(), counterParallelism).fieldsGrouping("ad-event-parser", new Fields("queryId", "adId"));
        builder.setBolt("rolling-ctr", new RollingCTRCalculatorBolt().withWindow(
            new BaseWindowedBolt.Count(2), 
            new BaseWindowedBolt.Count(1)
        ), rollingCounterParallelism).fieldsGrouping("counter", new Fields("queryId", "adId"));
        builder.setBolt("logger-bolt", new LoggerBolt(), 1).shuffleGrouping("rolling-ctr");
    }
}
