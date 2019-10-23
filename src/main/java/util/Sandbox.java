package util;

import model.Aggregate;
import model.AggregateValueTuple;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class Sandbox {
    public static void main(String[] args) {
        Date readingDate = new Date(1569324687747L);
        System.out.println("Minute rounded timestamp: " + DateUtils.truncate(readingDate, Calendar.MINUTE).getTime());
        System.out.println("Hour rounded timestamp: " + DateUtils.truncate(readingDate, Calendar.HOUR).getTime());
        System.out.println("Day rounded timestamp: " + DateUtils.truncate(readingDate, Calendar.DATE).getTime());
        System.out.println("Month rounded timestamp: " + DateUtils.truncate(readingDate, Calendar.MONTH).getTime());
        System.out.println("Year rounded timestamp: " + DateUtils.truncate(readingDate, Calendar.YEAR).getTime());
        //return reading.getGeohash().substring(0, GH_PRECISION) + "#" + reading.getTimestamp();

        // AggregateValueTuple and Aggregate
        AggregateValueTuple avt = new AggregateValueTuple();
        System.out.println(avt.toString());

        Aggregate agg = new Aggregate((long) 0,0.0,0.0);
        try {
            System.out.println(agg.getClass().getField("avg").get(agg));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

        // Stream.of(HashMap)
        HashMap<String,String> hm = new HashMap<>();

        hm.put("Cricket", "Sachin");
        hm.put("Football", "Zidane");
        hm.put("Tennis", "Federer");

        for (Map.Entry e : hm.entrySet()) {
            System.out.println(e);
        }

        hm.entrySet().stream()
                .forEach(System.out::println);
    }
}
