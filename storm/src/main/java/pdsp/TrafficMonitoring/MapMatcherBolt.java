package pdsp.TrafficMonitoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.net.URL;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MapMatcherBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = LoggerFactory.getLogger(MapMatcherBolt.class);
    public static Map<Long,Double[][]> roadMap = new HashMap<>();
    //creat roads list
    ArrayList<Road> roads = new ArrayList<Road>();
    //create roads
    Road road1 = new Road("1", 36.5, -98.5, 35.5, -97.5);
    Road road2 = new Road("2", 36.5, -97.5, 35.5, -96.5);
    Road road3 = new Road("3", 36.5, -96.5, 35.5, -95.5);
    Road road4 = new Road("4", 36.5, -95.5, 35.5, -94.5);


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        try {
            loadShapeFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //add roads to the list
        roads.add(road1);
        roads.add(road2);
        roads.add(road3);
        roads.add(road4);

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            TrafficEvent event = (TrafficEvent) tuple.getValueByField("trafficEvent");
            System.out.println("MapMatcherBolt received: " + event);
            // getting the road the vehicle is in
            DecimalFormat df = new DecimalFormat("#.##");
            double latitude = Double.parseDouble(df.format(event.getLatitude()));
            double longitude = Double.parseDouble(df.format(event.getLongitude()));
            for(Map.Entry<Long,Double[][]> entry : roadMap.entrySet()){
                Long roadId = entry.getKey();
                Double[][] coordinates = entry.getValue();
                int i = 0;
                for (Double[] coord : coordinates) {
                    i++;
                    if(coord[0].equals(longitude) && coord[1].equals(latitude)) {
                        Road road = new Road(String.valueOf(roadId), coord[1], coord[0], 0, 0);
                        System.out.println("Vehicle is in road: " + roadId);
                        System.out.println("iteration: " + i);
                        collector.emit(new Values(road, event));
                    }
                }
            }
            /*
            // check if vehicle is in road
            if (event.getLatitude() > 35.5 && event.getLatitude() < 36.5 && event.getLongitude() > -98.5 && event.getLongitude() < -97.5) {
                System.out.println("Vehicle is in city");
                //check which road the vehicle is in
                for (Road road : roads) {
                    if (event.getLatitude() > road.getMinLatitude() && event.getLatitude() < road.getMaxLatitude() && event.getLongitude() > road.getMinLongitude() && event.getLongitude() < road.getMaxLongitude()) {
                        System.out.println("Vehicle is in road: " + road.getRoadId());
                        collector.emit(new Values(road, event));
                    }
                }
                //emit road + data received, in another bolt we will calculate average speed of the road
            }
            */



        }
        catch (Exception e) {
            System.out.println("Error in MapMatcherBolt: " + e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("road", "trafficEvent", "e2eTimestamp", "processingTimestamp"));

    }




    public static void loadShapeFile() throws IOException {

        DecimalFormat df = new DecimalFormat("#.##");
        LOG.info("Loading shape file");
        String geoJsonUrl =  "https://gitlab.com/sumalya/cloudlab_profile2/-/raw/master/roads.geojson?ref_type=heads&inline=false";
        //loading the file
        URL url = new URL(geoJsonUrl);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            //googles Gson library
            Gson gson = new Gson();
            JsonObject jsonObject = gson.fromJson(reader, JsonObject.class);
            JsonArray featureArray = jsonObject.getAsJsonArray("features");
            for(int i = 0; i < featureArray.size(); i++){
                JsonObject featureObject = featureArray.get(i).getAsJsonObject();
                JsonObject props = featureObject.getAsJsonObject("properties");
                JsonArray coordinatesArray = featureObject.getAsJsonObject("geometry").getAsJsonArray("coordinates");
                Long osm_id = props.get("osm_id").getAsLong();
                Double[][] coordinates = new Double[coordinatesArray.size()][2];
                for (int j = 0; j < coordinatesArray.size(); j++) {
                    JsonArray coordinate = coordinatesArray.get(j).getAsJsonArray();
                    Double longitude = coordinate.get(0).getAsDouble();
                    Double latitude = coordinate.get(1).getAsDouble();
                    coordinates[j][0] = Double.parseDouble(df.format(longitude));
                    coordinates[j][1] = Double.parseDouble(df.format(latitude));
                }
                LOG.info("Osm_id:"+osm_id);
                roadMap.put(osm_id,coordinates);
            }
        } catch (Exception e){

        }

    }
}
