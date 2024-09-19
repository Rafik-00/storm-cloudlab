package pdsp.trendingTopics;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;

import java.util.concurrent.TimeUnit;

public class TrendingTopicsTopology extends AbstractTopology {

    int windowDurationSeconds;
    int topicPopularityThreshold;

    public TrendingTopicsTopology(String topologyName, String mode, String filePath, String kafkaTopic, int windowDurationSeconds, int topicPopularityThreshold,pdsp.config.Config config) {
        super(topologyName, mode, filePath, kafkaTopic,config);
        this.windowDurationSeconds = windowDurationSeconds;
        this.topicPopularityThreshold = topicPopularityThreshold;

    }

    @Override
    protected void buildTopology() {


        int twitterParserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int topicExtractorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int rollingCounterSlidingWindowParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int intermediateRankingParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int finalRankingParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((twitterParserParallelism + topicExtractorParallelism + rollingCounterSlidingWindowParallelism + intermediateRankingParallelism + finalRankingParallelism) / 4.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        BaseRichSpout spout = getSpout();

        builder.setSpout("source-spout", spout, parallelism);
        builder.setBolt("twitter-parser-bolt", new TwitterparserBolt(),twitterParserParallelism).shuffleGrouping("source-spout");
        builder.setBolt("topic-extractor-bolt", new TopicExtractorBolt(), topicExtractorParallelism).shuffleGrouping("twitter-parser-bolt");

        builder.setBolt(
                        "rolling-counter-sliding-window-bolt",
                        new RollingCounterSlidingWindowBolt().withWindow(
                                new BaseWindowedBolt.Duration(windowDurationSeconds, TimeUnit.SECONDS)
                        ),rollingCounterSlidingWindowParallelism
                )
                .fieldsGrouping("topic-extractor-bolt",new Fields("topic"));

        builder.setBolt("intermediate-ranking-bolt", new IntermediateRankingBolt(),intermediateRankingParallelism).shuffleGrouping("rolling-counter-sliding-window-bolt");
        builder.setBolt("final-ranking-bolt", new FinalRankingBolt(topicPopularityThreshold),finalRankingParallelism).globalGrouping("intermediate-ranking-bolt");

        builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                .shuffleGrouping("final-ranking-bolt");

    }

}
