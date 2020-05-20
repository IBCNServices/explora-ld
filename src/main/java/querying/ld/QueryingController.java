package querying.ld;

import com.google.common.collect.Sets;
import model.Aggregate;
import model.AggregateValueTuple;
import model.ErrorMessage;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.jackson.JacksonFeature;
import sun.reflect.generics.tree.Tree;
import util.Aggregator;
import util.AppConfig;
import util.HostStoreInfo;
import util.MetadataService;
import util.geoindex.QuadHash;
import util.geoindex.Tile;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryingController {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS");

    public QueryingController(KafkaStreams streams, HostInfo hostInfo) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
    }

    public TreeMap<String, Aggregate> solveSpatialQuery(Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metricId) {
        System.out.println("[solveSpatialQuery] method call");
//        String quadKey = QuadHash.getQuadKey(quadTile);
//        long ts = Instant.parse(page).toEpochMilli();
//        String searchKey = quadKey + "#" + toFormattedTimestamp(ts, ZoneId.systemDefault());
//        System.out.println("[solveSpatialQuery] ts=" + ts);
//        System.out.println("[solveSpatialQuery] searchKey=" + searchKey);

        if(!metricId.isEmpty()) {
            String viewStoreName = "view-" + metricId.replace("::", ".") + "-gh" + quadTile.getZoom() + "-" + aggrPeriod;
            return getLocalAggregates4MetricAndRange(viewStoreName, quadTile, page, aggrMethod, aggrPeriod, metricId);
        } else {
            Aggregator<String> aggCollect = AppConfig.SUPPORTED_METRICS.stream().map(metric -> {
                final String store = "view-" + metric.replace("::", ".") + "-gh" + quadTile.getZoom() + "-" + aggrPeriod;
                return fetchAggregate4MetricAndRange(store, quadTile, page, aggrMethod, aggrPeriod, metric);
            }).collect(Aggregator<String>::new, Aggregator<String>::accept, Aggregator<String>::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public TreeMap<String, Aggregate> fetchAggregate4MetricAndRange(String storeName, Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metricId) {
        List<HostStoreInfo> hosts = metadataService.streamsMetadataForStore(storeName);
        Aggregator<String> aggCollect = hosts.stream()
                .map(host -> {
                    if (!thisHost(host)) {
                        try {
                            System.out.println(String.format("[fetchAggregate4MetricAndRange] Forwarding request to %s:%s", host.getHost(), host.getPort()));
                            return client.target(String.format("http://%s:%d/data/%s/%s/%s",
                                    host.getHost(),
                                    host.getPort(),
                                    quadTile.getZoom(),
                                    quadTile.getX(),
                                    quadTile.getY()))
                                    .queryParam("page", page)
                                    .queryParam("aggrMethod", aggrMethod)
                                    .queryParam("aggrPeriod", aggrPeriod)
                                    .queryParam("metricId", metricId)
                                    .request(MediaType.APPLICATION_JSON_TYPE)
                                    .get(new GenericType<TreeMap<String, Aggregate>>() {});
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
                        return getLocalAggregates4MetricAndRange(storeName, quadTile, page, aggrMethod, aggrPeriod, metricId);
                    }
                })
                .collect(Aggregator<String>::new, Aggregator<String>::accept, Aggregator<String>::combine);
        return aggCollect.getAggregateMap();
    }

    public TreeMap<String, Aggregate> getLocalAggregates4MetricAndRange(String storeName, Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metricId) {
        System.out.println("[getLocalAggregates4MetricAndRange] look in the local store for metric: " + metricId + " (" + storeName + ")");
        final ReadOnlyKeyValueStore<String, AggregateValueTuple> viewStore = streams.store(storeName,
                QueryableStoreTypes.keyValueStore());
        String quadKey = QuadHash.getQuadKey(quadTile);
        final long fromTimestamp = Instant.parse(page).toEpochMilli();
        final long toTimestamp = fromTimestamp + getLDFragmentInterval() - getPeriodInterval(aggrPeriod);
        final String fromK = quadKey + "#" + toFormattedTimestamp(fromTimestamp, ZoneOffset.UTC);
        final String toK = quadKey + "#" + toFormattedTimestamp(toTimestamp, ZoneOffset.UTC);
        System.out.println("fromK=" + fromK);
        System.out.println("toK=" + toK);
        TreeMap<String, Aggregate> aggregateReadings = new TreeMap<>();
        KeyValueIterator<String, AggregateValueTuple> iterator =  viewStore.range(fromK, toK);
        while (iterator.hasNext()) {
            KeyValue<String, AggregateValueTuple> aggFromStore = iterator.next();
            System.out.println("Aggregate for " + aggFromStore.key + ": " + aggFromStore.value);
            Aggregate agg = new Aggregate(aggFromStore.value.count, aggFromStore.value.sum, aggFromStore.value.avg, aggFromStore.value.sensed_by);
            aggregateReadings.merge(metricId + "#" + aggFromStore.value.ts, agg,
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count), new HashSet<>(Stream.concat(a1.sensed_by.stream(), a2.sensed_by.stream()).collect(Collectors.toSet()))));
        }
        iterator.close();
        return aggregateReadings;
    }

    public static long getPeriodInterval(String timePeriod) {
        switch (timePeriod) {
            case "min":
                return 60000L;
            case "day":
                return 86400000L;
            case "month":
                return 2592000000L;
            default:
                return 3600000L;
        }
    }

//    public TreeMap<String, Aggregate> fetchAggregate(HostStoreInfo host, Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metric) {
//        try {
//                System.out.println(String.format("[fetchAggregate] Forwarding request to %s:%s", host.getHost(), host.getPort()));
//                return client.target(String.format("http://%s:%d/data/%s/%s/%s",
//                        host.getHost(),
//                        host.getPort(),
//                        quadTile.getZoom(),
//                        quadTile.getX(),
//                        quadTile.getY()))
//                        .queryParam("page", page)
//                        .queryParam("aggrMethod", aggrMethod)
//                        .queryParam("aggrPeriod", aggrPeriod)
//                        .queryParam("metricId", metric)
//                        .request(MediaType.APPLICATION_JSON_TYPE)
//                        .get(new GenericType<TreeMap<String, Aggregate>>() {});
//        } catch (Exception e) {
//            e.printStackTrace();
//            Throwable rootCause = ExceptionUtils.getRootCause(e);
//            rootCause.printStackTrace();
//            Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
//                        .entity(new ErrorMessage(rootCause.getMessage(), 500))
//                        .build();
//            throw new WebApplicationException(errorResp);
//        }
//    }
//
//    public TreeMap<String, Aggregate> getLocalAggregate(String storeName, String searchKey, String metricId) {
//        System.out.println("[getLocalAggregate] Processing request for " + metricId + " with " + searchKey + "...");
//        try {
//            final ReadOnlyKeyValueStore<String, AggregateValueTuple> viewStore = streams.store(storeName,
//                    QueryableStoreTypes.keyValueStore());
//            TreeMap<String, Aggregate> aggregateReadings = new TreeMap<>();
//            AggregateValueTuple aggregateVT = viewStore.get(searchKey);
//            //assert aggregateVT != null : "Data not found for the the provided parameters";
//            System.out.println("[getLocalAggregate] aggregateVT(searchKey)=" + aggregateVT);
//            if(aggregateVT != null) {
//                Aggregate agg = new Aggregate(aggregateVT.count, aggregateVT.sum, aggregateVT.avg, aggregateVT.sensed_by);
//                aggregateReadings.merge(metricId, agg,
//                        (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count), new HashSet<>(Stream.concat(a1.sensed_by.stream(), a2.sensed_by.stream()).collect(Collectors.toSet()))));
//            }
////            System.out.println("[getLocalAggregate] aggregateReadings=" + aggregateReadings);
//            return aggregateReadings;
//        } catch (Exception e) {
//            e.printStackTrace();
//            Throwable rootCause = ExceptionUtils.getRootCause(e);
//            rootCause.printStackTrace();
//            Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
//                    .entity(new ErrorMessage(rootCause.getMessage(), 500))
//                    .build();
//            throw new WebApplicationException(errorResp);
//        }
//    }

    public long truncateTS(long timestamp, String resolution) {
        try{
            ZonedDateTime tsDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            switch (resolution) {
                case "min":
                    return tsDate.truncatedTo(ChronoUnit.MINUTES).toInstant().toEpochMilli();
                case "hour":
                    return tsDate.truncatedTo(ChronoUnit.HOURS).toInstant().toEpochMilli();
                case "day":
                    return tsDate.truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli();
                case "month":
                    return tsDate.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).toInstant().toEpochMilli();
                case "year":
                    return tsDate.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1).toInstant().toEpochMilli();
                default:
                    return timestamp;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return timestamp;
        }
    }

    public static Long getLDFragmentInterval() {
        String lDFragmentResolution = System.getenv("LD_FRAGMENT_RES") != null ? System.getenv("LD_FRAGMENT_RES") : "hour";
        return getPeriodInterval(lDFragmentResolution);
    }

    private static String toFormattedTimestamp(Long timestamp, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    public boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }
}
