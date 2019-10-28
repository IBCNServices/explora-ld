package util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class AppConfig {
    public static List<String> SUPPORTED_AGGR = Arrays.asList("avg", "sum", "count");
    public static List<String> SUPPORTED_INTERVALS = Arrays.asList("5min", "1hour", "1day", "1week", "1month", "all");
    public static List<String> SUPPORTED_RESOLUTIONS = Arrays.asList("min", "hour", "day", "month", "year");
    public static List<Integer> SUPPORTED_GH_PRECISION = Arrays.asList(6, 7);
    public static HashMap<String, String> TIME_RANGES = new HashMap<String, String>(){{
        put("5min", "min");
        put("1hour", "min");
        put("1day", "hour");
        put("1week", "hour");
        put("1month", "day");
        put("all", "day");
    }};
}
