package model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AirQualityKeyedReading extends AirQualityReading {
    private String gh_ts;

    public AirQualityKeyedReading(Long tsReceivedMs, String metricId, Long timestamp, String sourceId, String geohash, Long h3Index, Double elevation, Object value, String timeUnit, String gh_ts) {
        super(tsReceivedMs, metricId, timestamp, sourceId, geohash, h3Index, elevation, value, timeUnit);
        this.gh_ts = gh_ts;
    }

    public AirQualityKeyedReading() {
        super();
    }

    public String getGh_ts() {
        return gh_ts;
    }

    public void setGh_ts(String gh_ts) {
        this.gh_ts = gh_ts;
    }
}
