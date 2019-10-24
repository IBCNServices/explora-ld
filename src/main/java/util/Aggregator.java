package util;

import model.Aggregate;
import model.AggregateValueTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Aggregator implements Consumer<Map<Long, Aggregate>> {
    Map<Long, Aggregate> aggregateMap = new HashMap<>();

    public Map<Long, Aggregate> getAggregateMap() {
        return aggregateMap;
    }

    @Override
    public void accept(Map<Long, Aggregate> aggMap) {
        for(Map.Entry<Long, Aggregate> entry : aggMap.entrySet()) {
            aggregateMap.merge(entry.getKey(), entry.getValue(),
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum) / (a1.count + a2.count))
            );
        }
    }

    public void combine(Aggregator other) {
        other.aggregateMap.forEach(
                (ts, agg) -> aggregateMap.merge(ts, agg,
                        (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)))
        );
    }
}
