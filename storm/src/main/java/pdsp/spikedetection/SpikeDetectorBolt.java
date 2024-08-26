package pdsp.spikedetection;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SpikeDetectorBolt extends BaseRichBolt {
    private double threshold;
    private OutputCollector collector;

    public SpikeDetectorBolt(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
     this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        AverageValueModel averageValue = (AverageValueModel) tuple.getValueByField("averageValue");
        int sensorId = averageValue.getSensorId();
        float voltage = averageValue.getCurrentValue();
        float average = averageValue.getAverageValue();

        float relativeDifference = Math.abs(voltage - average) / average;
        boolean isSpike = relativeDifference > threshold;

        if(isSpike){
            collector.emit(new Values(sensorId, voltage, average, isSpike, tuple.getValueByField("e2eTimestamp"), tuple.getValueByField("processingTimestamp")));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sensorId", "voltage", "average", "isSpike","e2eTimestamp","processingTimestamp"));
    }
}
