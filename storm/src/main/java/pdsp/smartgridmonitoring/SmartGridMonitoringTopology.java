package pdsp.smartgridmonitoring;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;

public class SmartGridMonitoringTopology extends AbstractTopology {

    public SmartGridMonitoringTopology(String topologyName, String mode, String filePath, String kafkaTopic,pdsp.config.Config config) {
        super(topologyName, mode, filePath, kafkaTopic,config);
    }

    @Override
    protected void buildTopology() {

        BaseRichSpout spout = getSpout();

        int houseEventParserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int globalMedianParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int plugMedianParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int outlierDetectorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int houseLoadPredictorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
        int plugLoadPredictorParallelism = this.parallelismEnumerator.getRandomParallelismHint();


        this.parallelism = (int) Math.round((houseEventParserParallelism + globalMedianParallelism + plugMedianParallelism + outlierDetectorParallelism + houseLoadPredictorParallelism + plugLoadPredictorParallelism) / 6.0);
        System.out.println("Parallelism in TOPOLOGY: " + this.parallelism);

        builder.setSpout("source-spout", spout,parallelism);
        builder.setBolt("house-event-parser-bolt", new HouseEventParserBolt().withWindow(new BaseWindowedBolt.Count(windowSize)),houseEventParserParallelism).shuffleGrouping("source-spout");


        //outlier detection
        builder.setBolt("global-median-bolt", new GlobalMedianBolt(), globalMedianParallelism).fieldsGrouping("house-event-parser-bolt",new Fields("house_id"));
        builder.setBolt("plug-median-bolt", new PlugMedianBolt(), plugMedianParallelism)
               .fieldsGrouping("house-event-parser-bolt", new Fields("house_id", "household_id", "plug_id"));
        builder.setBolt("outlier-detector-bolt", new OutlierDetectorBolt(), plugLoadPredictorParallelism).fieldsGrouping("global-median-bolt",new Fields("house_id")).fieldsGrouping("plug-median-bolt",new Fields("house_id"));


        // load prediction
        builder.setBolt("house-load-predictor-bolt", new HouseLoadPredictorBolt(),houseLoadPredictorParallelism)
                .fieldsGrouping("house-event-parser-bolt", new Fields("house_id"));
        builder.setBolt("plug-load-predictor-bolt", new PlugLoadPredictorBolt(),plugLoadPredictorParallelism)
                .fieldsGrouping("house-event-parser-bolt", new Fields("house_id", "household_id", "plug_id"));





        builder.setBolt("logger-bolt", new LoggerBolt(), 1)
                .shuffleGrouping("outlier-detector-bolt")
                .shuffleGrouping("house-load-predictor-bolt")
                .shuffleGrouping("plug-load-predictor-bolt");




    }
}
