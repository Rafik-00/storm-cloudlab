package pdsp.smartgridmonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class OutlierDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private double globalMedian;
    Map<String, Double> outliers;
    private String plugID;
    private Double plugMedian;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.outliers = new HashMap<>();
        this.globalMedian = -1.0;
        this.plugID = "notinitialized";
        this.plugMedian = -1.0;
    }

    @Override
    public void execute(Tuple tuple) {


        if (tuple.contains("globalMedian")) {

            globalMedian = tuple.getDoubleByField("globalMedian");

            //check if first arrived tuple was an outlier
            if (!this.plugID.equals("notinitialized") && !this.plugMedian.equals(-1.0)) {

                if (this.plugMedian > globalMedian)
                    collector.emit(new Values(this.plugID, this.plugMedian, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));

                this.plugID = "notinitialized";
                this.plugMedian = -1.0;
            }

            collector.ack(tuple);


        }

        if (tuple.contains("plugMedian")) {

            double plugMedian = tuple.getDoubleByField("plugMedian");
            String plugID = tuple.getStringByField("plugID");

            if (globalMedian > -1) {
                if (plugMedian > globalMedian) {
                    collector.emit(new Values(plugID, plugMedian, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));

                    System.out.println("outlier detected: " + plugID + " globalmedian " + globalMedian + " plugmedian " + plugMedian);

                }
            } else {
                this.plugID = plugID;
                this.plugMedian = plugMedian;
            }

            collector.ack(tuple);


        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("outlierID", "plugMedian", "e2eTimestamp", "processingTimestamp"));

    }
}
