package util;

import model.AirQualityReading;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

// Extracts the embedded timestamp of a record (giving you "event time" semantics).
public class TSExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        AirQualityReading reading = (AirQualityReading) record.value();
        if (reading != null) {
            long timestamp = reading.getTimestamp();
            if (timestamp < 0) {
                throw new RuntimeException("Invalid negative timestamp.");
            }
            return timestamp;
        } else {
            return record.timestamp();
        }
    }
}