package model;

import java.util.HashSet;

public class AggregateValueTuple extends Aggregate{
    public String gh_ts;
    public String gh;
    public Long ts;
    public String metricId;

    public AggregateValueTuple() {
        super();
    }

    public AggregateValueTuple(String gh_ts, String gh, Long ts, String metricId, Long count, Double sum, Double avg, HashSet<String> sensed_by) {
        super(count, sum, avg, sensed_by);
        this.gh_ts = gh_ts;
        this.gh = gh;
        this.ts = ts;
        this.metricId = metricId;
    }

    @Override
    public String toString() {
        return "AggregateValueTuple{" +
                "gh_ts='" + gh_ts + '\'' +
                ", gh='" + gh + '\'' +
                ", ts=" + ts +
                ", metricId=" + metricId +
                ", count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                ", sensed_by=" + sensed_by +
                '}';
    }
}
