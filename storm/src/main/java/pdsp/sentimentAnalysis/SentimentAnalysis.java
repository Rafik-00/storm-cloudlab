package pdsp.sentimentAnalysis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import pdsp.common.CustomKafkaSpout;
import pdsp.common.FileSpout;

public class SentimentAnalysis {

	public static void main(String[] args) {
		String mode = "kafka"; // TODO: set mode to "kafka" or "file"
		TopologyBuilder builder = new TopologyBuilder();
		BaseRichSpout spout = null;
		String filepath = "storm/src/main/resources/sentimentAnalysis/tweets.txt"; // TODO: set the path to the file
		int parallelism = 3; // TODO: set the parallelism

		Config config = new Config();
		config.setDebug(true);

		// check if mode is kafka or file, then set the spout accordingly
		if (mode.equalsIgnoreCase("kafka")) {
			spout = new CustomKafkaSpout("storm-kafka-group", "tweets-topic").getKafkaSpout();
		} else if (mode.equalsIgnoreCase("file")) {
			spout = new FileSpout(filepath);
		}

		builder.setSpout("twitter-spout", spout);

		builder.setBolt("parse-tweet-bolt", new TweetParserBolt(), parallelism).shuffleGrouping("twitter-spout");
		builder.setBolt("analyze-sentiment-bolt", new TweetAnalyzerBolt(), parallelism).shuffleGrouping("parse-tweet-bolt");

		LocalCluster cluster = new LocalCluster();

        try {
			System.out.println("Submitting topology");
			cluster.submitTopology("sentiment-analysis", config, builder.createTopology());
			cluster.activate("sentiment-analysis");
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        cluster.killTopology("sentiment-analysis");
		cluster.shutdown();
	}

}
