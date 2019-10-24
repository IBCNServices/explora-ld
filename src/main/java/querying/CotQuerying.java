package querying;

import model.Aggregate;
import model.AggregateValueTuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import util.Aggregator;
import util.HostStoreInfo;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CotQuerying {

    private CotQueryingService serviceInstance;

    CotQuerying(CotQueryingService serviceInstance) {
        this.serviceInstance = serviceInstance;
    }

    public Map<Long, Aggregate> solveSpatialQuery(String metricId, String aggregate, List<String> geohashes, String resolution, String source, int geohashPrecision, Boolean local) {
        System.out.println("[solveSpatialQuery] method call");
        final String viewStoreName = source.equals("tiles") ? "view-" + metricId.replace("::", ".") + "-gh" + geohashPrecision + "-" + resolution
                : "raw-" + metricId.replace("::", ".");
        if (local) {
            System.out.println("[solveSpatialQuery] Answering request for LOCAL state");
            Aggregator aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, null, null))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
            System.out.println(aggCollect.getAggregateMap());
            return aggCollect.getAggregateMap();
        } else {
            System.out.println("[solveSpatialQuery] Answering request for GLOBAL state");
            final List<HostStoreInfo> hosts = serviceInstance.getMetadataService().streamsMetadataForStore(viewStoreName);
            System.out.println("[solveSpatialQuery] Queryable hosts: ");
            hosts.forEach(host -> System.out.println(host.getHost() + ":" + host.getPort()));
            Aggregator aggCollect = hosts.stream()
                    .peek(host -> System.out.println(String.format("[solveSpatialQuery] Current host: %s:%s", host.getHost(), host.getPort())))
                    .map(host -> getAllAggregates4GeohashList(host, viewStoreName, metricId, aggregate, geohashes, resolution, source, geohashPrecision))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public Map<Long, Aggregate> getAllAggregates4GeohashList(HostStoreInfo host, String viewStoreName, String metricId, String aggregate, List<String> geohashes, String resolution, String source, int geohashPrecision) {
        System.out.println(geohashes);
        if (!serviceInstance.thisHost(host)) {
            System.out.println(String.format("[getAllAggregates4GeohashList] Forwarding request to %s:%s", host.getHost(), host.getPort()));
            return serviceInstance.getClient().target(String.format("http://%s:%d/api/airquality/%s/aggregate/%s",
                    host.getHost(),
                    host.getPort(),
                    metricId,
                    aggregate))
                    .queryParam("geohashes", String.join(",", geohashes))
                    .queryParam("src", source)
                    .queryParam("res", resolution)
                    .queryParam("gh_precision", geohashPrecision)
                    .queryParam("local", true)
                    .request(MediaType.APPLICATION_JSON_TYPE)
                    .get(new GenericType<Map<Long, Aggregate>>() {
                    });
        } else {
            // look in the local store
            System.out.println(String.format("[getAllAggregates4GeohashList] look in the local store (%s)", host));
            Aggregator aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, null, null))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public Map<Long, Aggregate> getLocalAggregates4Range(String storeName, String geohashPrefix, Long from, Long to) {
        final ReadOnlyKeyValueStore<String, AggregateValueTuple> viewStore = serviceInstance.getStreams().store(storeName,
                QueryableStoreTypes.keyValueStore());
        final String fromK = geohashPrefix + "#" + (from != null ? String.valueOf(from) : "");
        final String toK = geohashPrefix + "#" + (to != null ? String.valueOf(to) : String.valueOf(System.currentTimeMillis()));
        Map<Long, Aggregate> aggregateReadings = new TreeMap<>();
        KeyValueIterator<String, AggregateValueTuple> iterator =  viewStore.range(fromK, toK);
        while (iterator.hasNext()) {
            KeyValue<String, AggregateValueTuple> aggFromStore = iterator.next();
            System.out.println("Aggregate for " + aggFromStore.key + ": " + aggFromStore.value);
            Aggregate agg = new Aggregate(aggFromStore.value.count, aggFromStore.value.sum, aggFromStore.value.avg);
            aggregateReadings.merge(aggFromStore.value.ts, agg,
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)));
        }
        return aggregateReadings;
    }
}
