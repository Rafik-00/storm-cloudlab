package pdsp.utils;

public class KafkaUtils {
    /**
     * Parse the value of a tuple
     * If the received tuple comes from a kafka spout, it will have the following format:
     * <topic, partition, offset, key, value>
     * the value is the array of [file line, e2etimestamp] converted to string
     * we are interested in converting the array string back to an array
     * @param value the value of the tuple
     * @return the array of [file line, e2etimestamp]
     */
    public static Object[] parseValue(String value) {
        String [] arr = value.substring(1, value.length() - 1).split(",");
        long timestamp = arr[arr.length - 1].isEmpty() ? System.currentTimeMillis() : Long.parseLong(arr[arr.length - 1].trim());
        String tupleStr = "";
        for (int i = 0; i < arr.length - 1; i++) {
            tupleStr += arr[i];
            if (i != arr.length - 2) {
                tupleStr += ",";
            }
        }
        return new Object[]{tupleStr, timestamp};
    }
}
