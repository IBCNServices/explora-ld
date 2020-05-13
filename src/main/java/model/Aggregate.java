package model;

import java.util.HashSet;
import java.util.Set;

public class Aggregate extends Object{
    public Long count;
    public Double sum;
    public Double avg;
    public HashSet<String> sensed_by;

    public Aggregate() {
    }

    public Aggregate(Long count, Double sum, Double avg, HashSet<String> sensed_by) {
        this.count = count;
        this.sum = sum;
        this.avg = avg;
        this.sensed_by = sensed_by;
    }
    
    @Override
    public String toString() {
        return "Aggregate{" +
                "count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                ", sensed_by=" + sensed_by +
                '}';
    }
}
