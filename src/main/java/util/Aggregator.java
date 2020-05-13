package util;

import com.google.common.collect.Sets;
import model.Aggregate;
import model.AggregateValueTuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator<T> implements Consumer<Map<T, Aggregate>> {
    TreeMap<T, Aggregate> aggregateMap = new TreeMap<>();

    public TreeMap<T, Aggregate> getAggregateMap() {
        return aggregateMap;
    }

    @Override
    public void accept(Map<T, Aggregate> aggMap) {
        for(Map.Entry<T, Aggregate> entry : aggMap.entrySet()) {
            aggregateMap.merge(entry.getKey(), entry.getValue(),
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum) / (a1.count + a2.count), new HashSet<>(Stream.concat(a1.sensed_by.stream(), a2.sensed_by.stream()).collect(Collectors.toSet())))
            );
        }
    }

    public void combine(Aggregator<T> other) {
        other.aggregateMap.forEach(
                (ts, agg) -> aggregateMap.merge(ts, agg,
                        (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count), new HashSet<>(Stream.concat(a1.sensed_by.stream(), a2.sensed_by.stream()).collect(Collectors.toSet()))))
        );
    }
}
