package pdsp.sentimentAnalysis;

import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class SentimentAnalysisTopology extends AbstractTopology {

    public SentimentAnalysisTopology(String topologyName, String mode, String filePath, String kafkaTopic, Config config) {
        super(topologyName, mode, filePath, kafkaTopic, config);
    }

    @Override
    protected void buildTopology() {
        BaseRichSpout spout = getSpout();

        int parserBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int analyzerBoltParallelism = this.parallelismEnumerator.getRandomParallelismHint();

        this.parallelism = (int) Math.round((parserBoltParallelism + analyzerBoltParallelism) / 2.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        builder.setSpout("twitter-spout", spout);

        builder.setBolt("parse-tweet-bolt", new TweetParserBolt(), parserBoltParallelism).shuffleGrouping("twitter-spout");
        builder.setBolt("analyze-sentiment-bolt", new TweetAnalyzerBolt(), analyzerBoltParallelism).shuffleGrouping("parse-tweet-bolt");
        builder.setBolt("logger-bolt", new LoggerBolt()).shuffleGrouping("analyze-sentiment-bolt");
    }
}
