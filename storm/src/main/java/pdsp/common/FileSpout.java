package pdsp.common;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class FileSpout extends BaseRichSpout {
    private String inputPath;
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private JSONArray jsonArray;
    private String fileExtension;

    public FileSpout(String inputPath) {
        this.inputPath = inputPath;
        this.fileExtension = getFileExtension(new File(inputPath));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            if(fileExtension.equals("csv") || fileExtension.equals("txt"))
                reader = new BufferedReader(new FileReader(inputPath));
            else if(fileExtension.equals("json")) {
                reader = new BufferedReader(new FileReader(inputPath));
                JSONParser parser = new JSONParser();
                jsonArray = (JSONArray) parser.parse(reader);
            } else {
                throw new RuntimeException("File extension not supported. Only txt, csv and JSON are supported.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading file [" + inputPath + "]", e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = null;
            if(fileExtension.equals("csv") || fileExtension.equals("txt")){
                line = reader.readLine();
            } else if(fileExtension.equals("json")) {
                if(!jsonArray.isEmpty()) {
                    line = jsonArray.get(0).toString();
                    jsonArray.remove(0);
                }
            }
            if (line != null) {
                System.out.println("Reading line: "+ line);
                long timestamp = System.currentTimeMillis();
                collector.emit(new Values(line, timestamp));
            } else {
                Utils.sleep(100);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line", "e2eTimestamp"));
    }

    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error closing reader", e);
        }
    }

    private String getFileExtension(File file) {
        String name = file.getName();
        int lastIndexOf = name.lastIndexOf(".");
        if (lastIndexOf == -1) {
            return ""; // empty extension
        }
        return name.substring(lastIndexOf+1);
    }
}
