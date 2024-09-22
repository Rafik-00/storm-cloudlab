package pdsp.clickAnalytics;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class ClickAnalyticsTopology extends AbstractTopology {
    int slidingWindowSize;
    int slidingWindowSlide;

    public ClickAnalyticsTopology(String topologyName, String mode, String filepath, String kafkaTopic, int slidingWindowSize, int slidingWindowSlide, Config config) {
        super(topologyName, mode, filepath, kafkaTopic, config);
        this.slidingWindowSize = slidingWindowSize;
        this.slidingWindowSlide = slidingWindowSlide;
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int parserOperatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int repeatVisitOperatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int sumReducerParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int geographyOperatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((parserOperatorParallelism + repeatVisitOperatorParallelism + sumReducerParallelism + geographyOperatorParallelism) / 4.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        config.registerSerialization(pdsp.clickAnalytics.ClickLog.class);
        config.registerSerialization(pdsp.clickAnalytics.GeoStats.class);
        builder.setSpout("click-log-spout", spout, parallelism);
        builder.setBolt("parse-click-log-bolt",new ClickLogParserBolt(), parserOperatorParallelism).shuffleGrouping("click-log-spout");

        // Query 1
        builder.setBolt("repeat-visit-operator-bolt",
                new RepeatVisitOperatorBolt().withWindow(
                        new BaseWindowedBolt.Count(slidingWindowSize),
                        new BaseWindowedBolt.Count(slidingWindowSlide)
                ),
                repeatVisitOperatorParallelism
        ).fieldsGrouping("parse-click-log-bolt", new Fields("clientKey"));

        builder.setBolt(
                "sum-reducer-bolt",
                new SumReducerBolt().withWindow(
                        new BaseWindowedBolt.Count(slidingWindowSize),
                        new BaseWindowedBolt.Count(slidingWindowSlide)
                ),
                sumReducerParallelism
        ).fieldsGrouping("repeat-visit-operator-bolt", new Fields("url"));

        // Query 2
        builder.setBolt(
                "geography-operator-bolt",
                new GeographyOperatorBolt(),
                geographyOperatorParallelism
        ).shuffleGrouping("parse-click-log-bolt");

        builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                .shuffleGrouping("sum-reducer-bolt")
                .shuffleGrouping("geography-operator-bolt");
    }
}
