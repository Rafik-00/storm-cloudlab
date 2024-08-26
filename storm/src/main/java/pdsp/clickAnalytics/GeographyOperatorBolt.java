package pdsp.clickAnalytics;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;

public class GeographyOperatorBolt extends BaseRichBolt {
    private transient DatabaseReader databaseReader;
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        try {
            URL databaseUrl = new URL("https://git.io/GeoLite2-City.mmdb");
            databaseReader = new DatabaseReader.Builder(databaseUrl.openStream()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
//        String ipAddress = tuple.getStringByField("ip");
        ClickLog clickLog = (ClickLog) tuple.getValueByField("clickLog");
        String ipAddress = clickLog.getIp();

        try {
            String city = performCityLookup(ipAddress);
            String country = performCountryLookup(ipAddress);

            GeoStats geoStats = new GeoStats(country);
            geoStats.updateCityVisits(city);
            outputCollector.emit(new Values(country, city, geoStats, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
        } catch (AddressNotFoundException e) {
            System.out.println("Address not found");
            GeoStats geoStats = new GeoStats("India");
            geoStats.updateCityVisits("Bangalore");
            outputCollector.emit(new Values("India", "Bangalore", geoStats, tuple.getLongByField("e2eTimestamp"), tuple.getLongByField("processingTimestamp")));
        } catch (IOException | GeoIp2Exception e) {
            e.printStackTrace();
        }
    }

    private String performCityLookup(String ipAddress) throws IOException, GeoIp2Exception {
        InetAddress inetAddress = InetAddress.getByName(ipAddress);
        CityResponse response = databaseReader.city(inetAddress);
        City city = response.getCity();
        return city.getName();
    }

    private String performCountryLookup(String ipAddress) throws IOException, GeoIp2Exception {
        InetAddress inetAddress = InetAddress.getByName(ipAddress);
        CityResponse response = databaseReader.city(inetAddress);
        Country country = response.getCountry();
        return country.getName();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("country","city", "geoStats", "e2eTimestamp", "processingTimestamp"));
    }

    @Override
    public void cleanup() {
        if (databaseReader != null) {
            try {
                databaseReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        super.cleanup();
    }
}
