package pdsp.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class JsonUtils {
    public static String convertMapToJson(Map<String, Object> map) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(map);
    }

    public static void saveJsonToFile(String json, String filePath) throws IOException {
        File file = new File(filePath);

        // Check if the file exists, if not create it along with any necessary parent directories
        if (!file.exists()) {
            file.getParentFile().mkdirs(); // Create parent directories if necessary
            file.createNewFile(); // Create the file if it does not exist
        }
        System.out.println("File path: " + file.getAbsolutePath());


        // Use ObjectMapper to write JSON string to file
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(file, json);
    }
}