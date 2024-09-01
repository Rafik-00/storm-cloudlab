package pdsp.utils;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * This class is used to read files of extension txt, csv and json.
 * It is used by the FileSpout class to read the file and emit tuples to the topology.
 * It is also used by the KafkaFileReadTupleProducer class to read the file and emit tuples through Kafka.
 */
public class CustomFileReader implements Serializable {
    private final String inputPath;
    private BufferedReader reader;
    private JSONArray jsonArray;
    private final String fileExtension;
    private final Logger logger = LoggerFactory.getLogger("file-reader");

    public CustomFileReader(String inputPath) {
        this.inputPath = inputPath;
        this.fileExtension = getFileExtension(new java.io.File(inputPath));
    }

    public void open() {
        try {
            resetFile();
        } catch (Exception e) {
            throw new RuntimeException("Error reading file [" + inputPath + "]", e);
        }
    }

    public String readNextTuple() {
        String line = null;
        try {
            if(fileExtension.equals("csv") || fileExtension.equals("txt")){
                line = reader.readLine();
            } else if(fileExtension.equals("json")) {
                if(!jsonArray.isEmpty()) {
                    line = jsonArray.get(0).toString();
                    jsonArray.remove(0);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        }
        return line;
    }

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

    public void resetFile() {
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
}
