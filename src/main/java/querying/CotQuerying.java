package querying;

import model.AggregateValueTuple;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import util.Aggregator;
import util.HostStoreInfo;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CotQuerying {

    private CotQueryingService serviceInstance;

    CotQuerying(CotQueryingService serviceInstance) {
        this.serviceInstance = serviceInstance;
    }

    public List<AggregateValueTuple> solveSpatialQuery(String metricId, String aggregate, List<String> geohashes, String resolution, String source, int geohashPrecision) {
        System.out.println("[solveSpatialQuery] method call");
        final List<AggregateValueTuple> results = new ArrayList<>();
        Aggregator aggCollect= (Aggregator) geohashes.stream()
                .flatMap(gh -> Stream.of(getAggregates4Geohash(metricId, aggregate, gh, resolution, source, geohashPrecision)))
                .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);

        return results;
    }

    public List<AggregateValueTuple> getAggregates4Geohash(String metricId, String aggregate, String geohash, String resolution, String source, int geohashPrecision) {
        final List<AggregateValueTuple> results = new ArrayList<>();
        final String viewStoreName = source == "tiles" ? "view-" + metricId.replace("::", ".") + "-gh" + geohashPrecision + "-" + resolution
                : "raw-" + metricId.replace("::", ".");
        final List<HostStoreInfo> hosts = serviceInstance.getMetadataService().streamsMetadataForStore(viewStoreName);
        System.out.println(hosts);
        for(HostStoreInfo host: hosts) {
            List<AggregateValueTuple> aggregateReadings = new ArrayList<>();
            if (!serviceInstance.thisHost(host)) {
                aggregateReadings = serviceInstance.getClient().target(String.format("http://%s:%d/api/airquality/%s/aggregate/%s",
                        host.getHost(),
                        host.getPort(),
                        metricId,
                        aggregate))
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .get(new GenericType<List<AggregateValueTuple>>() {
                        });
            } else {
                // look in the local store
                aggregateReadings = getLocalAggregates4Range(viewStoreName, geohash, null, null);
            }
            results.addAll(aggregateReadings);
        }

    }

    public List<AggregateValueTuple> getLocalAggregates4Range(String storeName, String geohashPrefix, Long from, Long to) {
        final ReadOnlyKeyValueStore<String, AggregateValueTuple> viewStore = serviceInstance.getStreams().store(storeName,
                QueryableStoreTypes.keyValueStore());
        final String fromK = geohashPrefix + "#" + (from != null ? String.valueOf(from) : "");
        final String toK = geohashPrefix + "#" + (to != null ? String.valueOf(to) : String.valueOf(System.currentTimeMillis()));
        List<AggregateValueTuple> aggregateReadings = new ArrayList<>();
        KeyValueIterator<String, AggregateValueTuple> iterator =  viewStore.range(fromK, toK);
        while (iterator.hasNext()) {
            KeyValue<String, AggregateValueTuple> aggReading = iterator.next();
            System.out.println("Aggregate for " + aggReading.key + ": " + aggReading.value);
            aggregateReadings.add(aggReading.value);
        }
        return aggregateReadings;
    }
}
