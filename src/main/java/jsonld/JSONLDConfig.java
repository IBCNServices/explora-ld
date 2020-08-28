package jsonld;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class JSONLDConfig {
    private static ObjectMapper mapper = new ObjectMapper();
    public static LinkedHashMap<String, Object> CONTEXT;
    public static List<LinkedHashMap<String, Object>> HYDRA_MAPPING;

    static {
        try {
            CONTEXT = mapper.readValue(JSONLDConfig.class.getClassLoader().getResourceAsStream("jsonLDContext.json"), new TypeReference<LinkedHashMap<String, Object>>() {});
            HYDRA_MAPPING = mapper.readValue(JSONLDConfig.class.getClassLoader().getResourceAsStream("hydraMapping.json"), new TypeReference<List<LinkedHashMap<String, Object>>>() {});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String FEATURE_OF_INTEREST = "AirQuality";
    public static String BASE_URL = "http://example.org/data/";
    //    public static openObeliskAddress = "http://localhost:5000";
//    // intervals to calculate averages
//    public static readonly minuteInterval: number = 60000;
//    public static readonly hourInterval: number = 3600000;



}






