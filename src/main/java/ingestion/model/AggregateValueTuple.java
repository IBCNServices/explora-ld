package ingestion.model;

public class AggregateValueTuple {
    public Long count;
    public Double sum;
    public Double avg;

    public AggregateValueTuple(long c, double s, double a) {
        count = c;
        sum = s;
        avg = a;
    }

    @Override
    public String toString() {
        return "AggregateValueTuple{" +
                "count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                '}';
    }
}
