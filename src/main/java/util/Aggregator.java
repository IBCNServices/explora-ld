package util;

import model.Aggregate;
import model.AggregateValueTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class Aggregator implements Consumer<List<AggregateValueTuple>> {
    Map<Long, Aggregate> aggregateMap = new HashMap<>();

    @Override
    public void accept(List<AggregateValueTuple> aggregateValueTuples) {
        for(AggregateValueTuple avt : aggregateValueTuples) {
            Aggregate aggregate = new Aggregate(avt.count, avt.sum, avt.avg);
            aggregateMap.merge(avt.ts, aggregate,
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum) / (a1.count + a2.count))
            );
        }
    }

    public void combine(Aggregator other) {
        aggregateMap.forEach(
                (ts, agg) -> other.aggregateMap.merge(ts, agg,
                        (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)))
        );
    }
}
