package jsonld;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class JSONLDConfig {
    private static JSONLDConfig instance;
    public static HashMap CONTEXT;
    public static String FEATURE_OF_INTEREST = "AirQuality";
    public static String BASE_URL = "http://example.org/data/";
    //    public static openObeliskAddress = "http://localhost:5000";
//    // intervals to calculate averages
//    public static readonly minuteInterval: number = 60000;
//    public static readonly hourInterval: number = 3600000;

    private JSONLDConfig(){
        ObjectMapper mapper = new ObjectMapper();
        try {
            System.out.println("Resource URL: " + JSONLDConfig.class.getResource("jsonLDContext.json"));
            CONTEXT = mapper.readValue(new File(
                    JSONLDConfig.class.getResource("jsonLDContext.json").getFile()), new TypeReference<HashMap<String, Object>>() {
            });
            System.out.println("INITIALIZING CONTEXT ... ");
            System.out.println(CONTEXT);
        } catch (IOException | NullPointerException e) {
            System.out.println("Resource URL: " + JSONLDConfig.class.getResource("jsonLDContext.json"));
            e.printStackTrace();
        }

    }

    public static JSONLDConfig getInstance(){
        if(instance == null){
            synchronized (JSONLDConfig.class) {
                if(instance == null){
                    instance = new JSONLDConfig();
                }
            }
        }
        return instance;
    }

}






