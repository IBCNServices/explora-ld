package querying;

import model.Aggregate;
import model.AggregateValueTuple;
import model.ErrorMessage;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.jackson.JacksonFeature;
import util.Aggregator;
import util.AppConfig;
import util.HostStoreInfo;
import util.MetadataService;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CotQuerying {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();

    public CotQuerying(KafkaStreams streams, HostInfo hostInfo) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
    }

    public TreeMap<Long, Aggregate> solveSpatialQuery(String metricId, String aggregate, List<String> geohashes, String resolution, Long fromDate, Long toDate, String source, int geohashPrecision, Boolean local) {
        System.out.println("[solveSpatialQuery] method call");
        final String viewStoreName = source.equals("tiles") ? "view-" + metricId.replace("::", ".") + "-gh" + geohashPrecision + "-" + resolution
                : "raw-" + metricId.replace("::", ".");
        if (local) {
            System.out.println(String.format("[solveSpatialQuery] Answering request for LOCAL state (addressed to host %s:%s)", hostInfo.host(), hostInfo.port()));
            Aggregator aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, fromDate <= 0 ? null : fromDate, toDate <= 0 ? null : toDate))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
//            System.out.println("[solveSpatialQuery] aggCollect.getAggregateMap(): " +aggCollect.getAggregateMap());
            return aggCollect.getAggregateMap();
        } else {
            System.out.println(String.format("[solveSpatialQuery] Answering request for GLOBAL state (addressed to host %s:%s)", hostInfo.host(), hostInfo.port()));
            final List<HostStoreInfo> hosts = metadataService.streamsMetadataForStore(viewStoreName);
            System.out.println("[solveSpatialQuery] Queryable hosts: ");
            hosts.forEach(host -> System.out.println(host.getHost() + ":" + host.getPort()));
            Aggregator aggCollect = hosts.stream()
                    .peek(host -> System.out.println(String.format("[solveSpatialQuery] Current host: %s:%s", host.getHost(), host.getPort())))
                    .map(host -> getAggregates4GeohashList(host, viewStoreName, metricId, aggregate, geohashes, resolution, "", source, fromDate, toDate, geohashPrecision))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public TreeMap<Long, Aggregate> solveSpatioTemporalQuery(String metricId, String aggregate, List<String> geohashes, String interval, Long fromDate, String source, int geohashPrecision, Boolean local) {
        System.out.println("[solveSpatioTemporalQuery] method call");
        String resolution = AppConfig.TIME_RANGES.get(interval);
        final String viewStoreName = source.equals("tiles") ? "view-" + metricId.replace("::", ".") + "-gh" + geohashPrecision + "-" + resolution
                : "raw-" + metricId.replace("::", ".");
        if (local) {
            Long to = fromDate <= 0 ? System.currentTimeMillis() : fromDate;
            Long from = getFromDate(to, interval);
            System.out.println(String.format("[solveSpatioTemporalQuery] Answering request for LOCAL state (addressed to host %s:%s)", hostInfo.host(), hostInfo.port()));
            Aggregator aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, from, to))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
//            System.out.println("[solveSpatialQuery] aggCollect.getAggregateMap(): " +aggCollect.getAggregateMap());
            return aggCollect.getAggregateMap();
        } else {
            System.out.println(String.format("[solveSpatioTemporalQuery] Answering request for GLOBAL state (addressed to host %s:%s)", hostInfo.host(), hostInfo.port()));
            final List<HostStoreInfo> hosts = metadataService.streamsMetadataForStore(viewStoreName);
            System.out.println("[solveSpatioTemporalQuery] Queryable hosts: ");
            hosts.forEach(host -> System.out.println(host.getHost() + ":" + host.getPort()));
            Aggregator aggCollect = hosts.stream()
                    .peek(host -> System.out.println(String.format("[solveSpatioTemporalQuery] Current host: %s:%s", host.getHost(), host.getPort())))
                    .map(host -> getAggregates4GeohashList(host, viewStoreName, metricId, aggregate, geohashes, "", interval, source, fromDate, -1L, geohashPrecision))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public Map<Long, Aggregate> getAggregates4GeohashList(HostStoreInfo host, String viewStoreName, String metricId, String aggregate, List<String> geohashes, String resolution, String interval, String source, Long fromDate, Long toDate, int geohashPrecision) {
        System.out.println(geohashes);
        String tmp_predicate_key = resolution.isEmpty() ? "interval" : "res";
        String tmp_predicate_value = resolution.isEmpty() ? interval : resolution;
        if (!thisHost(host)) {
            try {
                System.out.println(String.format("[getAggregates4GeohashList] Forwarding request to %s:%s", host.getHost(), host.getPort()));
                //                System.out.println(aggregateReadings);
                return client.target(String.format("http://%s:%d/api/airquality/%s/aggregate/%s/history",
                        host.getHost(),
                        host.getPort(),
                        metricId,
                        aggregate))
                        .queryParam("geohashes", String.join(",", geohashes))
                        .queryParam("src", source)
                        .queryParam(tmp_predicate_key, tmp_predicate_value)
                        .queryParam("gh_precision", geohashPrecision)
                        .queryParam("from", fromDate)
                        .queryParam("to", toDate)
                        .queryParam("local", true)
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .get(new GenericType<TreeMap<Long, Aggregate>>() {});
            } catch (Exception e) {
                e.printStackTrace();
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                rootCause.printStackTrace();
                Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorMessage(rootCause.getMessage(), 500))
                        .build();
                throw new WebApplicationException(errorResp);
            }
        } else {
            // look in the local store
            Long to, from;
            if (resolution.isEmpty()) {
                to = fromDate <= 0 ? System.currentTimeMillis() : fromDate;
                from = getFromDate(to, interval);
            } else {
                to = toDate <= 0 ? null : toDate;
                from = fromDate <= 0 ? null : fromDate;
            }
            System.out.println("from: " + from);
            System.out.println("to: " + to);
            System.out.println(String.format("[getAggregates4GeohashList] look in the local store (%s:%s)", host.getHost(), host.getPort()));
            Aggregator aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, from, to))
                    .collect(Aggregator::new, Aggregator::accept, Aggregator::combine);
//          System.out.println("[getAllAggregates4GeohashList] aggCollect.getAggregateMap(): " + aggCollect.getAggregateMap());
            return aggCollect.getAggregateMap();
        }
    }

    public Map<Long, Aggregate> getLocalAggregates4Range(String storeName, String geohashPrefix, Long from, Long to) {
        final ReadOnlyKeyValueStore<String, AggregateValueTuple> viewStore = streams.store(storeName,
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

    private Long getFromDate(long toDate, String interval) {
        //"5min", "1hour", "1day", "1week", "1month", "all"
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(toDate);
        switch (interval) {
            case "1hour":
                c.add(Calendar.HOUR, -1);
                return c.getTime().getTime();
            case "1day":
                c.add(Calendar.DATE, -1);
                return c.getTime().getTime();
            case "1week":
                c.add(Calendar.DATE, -7);
                return c.getTime().getTime();
            case "1month":
                c.add(Calendar.MONTH, -1);
                return c.getTime().getTime();
            case "all":
                c.add(Calendar.YEAR, -30);
                return c.getTime().getTime();
            default:
                c.add(Calendar.MINUTE, -5);
                return c.getTime().getTime();
        }
    }

    public boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }
}
