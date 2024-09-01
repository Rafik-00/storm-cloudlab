package pdsp.common;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import pdsp.utils.CustomFileReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Map;

public class FileSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final CustomFileReader customFileReader;

    public FileSpout(String inputPath) {
        this.customFileReader = new CustomFileReader(inputPath);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        customFileReader.open();
    }

    @Override
    public void nextTuple() {
        String line = customFileReader.readNextTuple();
        if (line != null) {
            collector.emit(new Values(line, System.currentTimeMillis()));
        } else {
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line", "e2eTimestamp"));
    }

    @Override
    public void close() {
        customFileReader.close();
    }
}
