package model;

public class AggregateValueTuple {
    public String gh_ts;
    public String gh;
    public Long ts;
    public Long count;
    public Double sum;
    public Double avg;

    public AggregateValueTuple() {
        super();
    }

    public AggregateValueTuple(String gh_ts, String gh, Long ts, Long count, Double sum, Double avg) {
        this.gh_ts = gh_ts;
        this.gh = gh;
        this.ts = ts;
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "AggregateValueTuple{" +
                "gh_ts='" + gh_ts + '\'' +
                ", gh='" + gh + '\'' +
                ", ts=" + ts +
                ", count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                '}';
    }
}
