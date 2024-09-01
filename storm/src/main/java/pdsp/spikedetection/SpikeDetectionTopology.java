package pdsp.spikedetection;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import pdsp.common.AbstractTopology;
import pdsp.common.LoggerBolt;
import pdsp.config.Config;

public class SpikeDetectionTopology extends AbstractTopology{
    private double threshold;
    
   public SpikeDetectionTopology(String topologyName, String mode, String filePath, String kafkaTopic, double threshold, Config config){
        super(topologyName, mode, filePath, kafkaTopic, config);
        this.threshold = threshold;
   }

   @Override
   protected void buildTopology(){
    BaseRichSpout spout = getSpout();

    int parserParallelism = this.parallelismEnumerator.getRandomParallelismHint();
    int averageCalculatorParallelism = this.parallelismEnumerator.getRandomParallelismHint();
    int spikeDetectorParallelism = this.parallelismEnumerator.getRandomParallelismHint();

    this.parallelism = (int)Math.round((parserParallelism + averageCalculatorParallelism + spikeDetectorParallelism) /3.0);

    // Define the topology
    builder.setSpout("source-spout", spout);
    builder.setBolt("parser-bolt", new SensorMeasurementParserBolt(), parserParallelism).shuffleGrouping("source-spout");
    builder.setBolt("average-calculator-bolt", new AverageCalculatorBolt().withWindow(
            new BaseWindowedBolt.Count(windowSize), 
            new BaseWindowedBolt.Count(slidingInterval)  
    ), averageCalculatorParallelism).fieldsGrouping("parser-bolt", new Fields("sensorId"));
    builder.setBolt("spike-detector-bolt", new SpikeDetectorBolt(threshold), spikeDetectorParallelism).fieldsGrouping("average-calculator-bolt", new Fields("sensorId"));
    builder.setBolt("logger-bolt", new LoggerBolt(), 1).shuffleGrouping("spike-detector-bolt");
   }
}
