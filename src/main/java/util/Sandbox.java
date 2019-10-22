package util;

import model.AggregateValueTuple;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

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
    }
}
