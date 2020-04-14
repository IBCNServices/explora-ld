package model;

public class AggregateValueTuple extends Aggregate{
    public String gh_ts;
    public String gh;
    public Long ts;

    public AggregateValueTuple() {
        super();
    }

    public AggregateValueTuple(String gh_ts, String gh, Long ts, Long count, Double sum, Double avg) {
        super(count, sum, avg);
        this.gh_ts = gh_ts;
        this.gh = gh;
        this.ts = ts;
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
