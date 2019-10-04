package model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AirQualityReading {
    private Long tsReceivedMs;
    private String metricId;
    private Long timestamp;
    private String sourceId;
    private String geohash;
    private Long h3Index;
    private Double elevation;
    private Object value;
    private String timeUnit;

    public AirQualityReading() {
    }

    public AirQualityReading(Long tsReceivedMs, String metricId, Long timestamp, String sourceId, String geohash, Long h3Index, Double elevation, Object value, String timeUnit) {
        this.tsReceivedMs = tsReceivedMs;
        this.metricId = metricId;
        this.timestamp = timestamp;
        this.sourceId = sourceId;
        this.geohash = geohash;
        this.h3Index = h3Index;
        this.elevation = elevation;
        this.value = value;
        this.timeUnit = timeUnit;
    }

    public Long getTsReceivedMs() {
        return tsReceivedMs;
    }

    public void setTsReceivedMs(Long tsReceivedMs) {
        this.tsReceivedMs = tsReceivedMs;
    }

    public String getMetricId() {
        return metricId;
    }

    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public Long getH3Index() {
        return h3Index;
    }

    public void setH3Index(Long h3Index) {
        this.h3Index = h3Index;
    }

    public Double getElevation() {
        return elevation;
    }

    public void setElevation(Double elevation) {
        this.elevation = elevation;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = timeUnit;
    }

    @Override
    public String toString() {
        return "AirQualityReading{" +
                "tsReceivedMs=" + tsReceivedMs +
                ", metricId='" + metricId + '\'' +
                ", timestamp=" + timestamp +
                ", sourceId='" + sourceId + '\'' +
                ", geohash='" + geohash + '\'' +
                ", h3Index=" + h3Index +
                ", elevation=" + elevation +
                ", value=" + value +
                ", timeUnit='" + timeUnit + '\'' +
                '}';
    }
}
