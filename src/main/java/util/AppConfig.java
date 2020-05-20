package util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AppConfig {
    public static List<String> SUPPORTED_AGGR = Arrays.asList("avg", "sum", "count");
    public static List<String> SUPPORTED_INTERVALS = Arrays.asList("5min", "1hour", "1day", "1week", "1month", "all");
    public static List<String> SUPPORTED_RESOLUTIONS = Arrays.asList("min", "hour", "day", "month");
    public static List<Integer> SUPPORTED_PRECISION = System.getenv("PRECISION") != null ? Stream.of(System.getenv("PRECISION").split(",")).map(gh -> Integer.parseInt(gh)).collect(Collectors.toList()): Arrays.asList(6,7);
    public static List<String> SUPPORTED_METRICS = System.getenv("METRICS") != null ? Stream.of(System.getenv("METRICS").split(",")).collect(Collectors.toList()) : Arrays.asList("airquality.no2::number","airquality.pm10::number");
    public static List<String> SUPPORTED_GEO_INDEXING = Arrays.asList("geohashing", "quadtiling");
    public static String LD_FRAGMENT_RES = System.getenv("LD_FRAGMENT_RES") != null ? System.getenv("LD_FRAGMENT_RES") : "hour";
    public static HashMap<String, String> TIME_RANGES = new HashMap<String, String>(){{
        put("5min", "min");
        put("1hour", "min");
        put("1day", "hour");
        put("1week", "hour");
        put("1month", "day");
        put("all", "day");
    }};
}
