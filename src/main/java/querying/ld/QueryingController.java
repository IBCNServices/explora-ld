package querying.ld;

import model.Aggregate;
import model.AggregateValueTuple;
import model.ErrorMessage;
import model.TSFragmentData;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.glassfish.jersey.jackson.JacksonFeature;
import util.*;
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
import java.util.*;
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

    public List<LinkedHashMap<String, Object>> solveSpatialQuery(Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metricId) {
        System.out.println("[solveSpatialQuery] method call");
        if(!metricId.isEmpty()) {
            String viewStoreName = "view-" + metricId.replace("::", ".") + "-gh" + quadTile.getZoom() + "-" + aggrPeriod;
            return getLocalAggregates4MetricAndPage(viewStoreName, quadTile, page, aggrMethod, aggrPeriod, metricId);
        } else {
            TSFragmentsAggregator aggCollect = AppConfig.SUPPORTED_METRICS.stream().map(metric -> {
                final String store = "view-" + metric.replace("::", ".") + "-gh" + quadTile.getZoom() + "-" + aggrPeriod;
                String quadKey = QuadHash.getQuadKey(quadTile);
                long ts = Instant.parse(page).toEpochMilli();
                String searchKey = quadKey + "#" + toFormattedTimestamp(ts, ZoneId.of("UTC"));
//                System.out.println("[solveSpatialQuery] metricId=" + metric + " ts=" + ts);
//                System.out.println("[solveSpatialQuery] searchKey=" + searchKey);
                final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(store, searchKey, new StringSerializer());
                if (!thisHost(host)) {
                    return fetchAggregate4MetricAndPage(host, store, quadTile, page, aggrMethod, aggrPeriod, metric);
                } else {
                    return getLocalAggregates4MetricAndPage(store, quadTile, page, aggrMethod, aggrPeriod, metricId);
                }
            }).collect(TSFragmentsAggregator::new, TSFragmentsAggregator::accept, TSFragmentsAggregator::combine);
            return aggCollect.getAggregateList();
        }
    }

    public List<LinkedHashMap<String, Object>> fetchAggregate4MetricAndPage(HostStoreInfo host, String storeName, Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metricId) {
        try {
                System.out.println(String.format("[fetchAggregate4MetricAndPage] Forwarding request to %s:%s", host.getHost(), host.getPort()));
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
                        .get(new GenericType<List<LinkedHashMap<String, Object>>>() {});
        } catch (Exception e) {
            e.printStackTrace();
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            rootCause.printStackTrace();
            Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorMessage(rootCause.getMessage(), 500))
                    .build();
            throw new WebApplicationException(errorResp);
        }
    }

    public List<LinkedHashMap<String, Object>> getLocalAggregates4MetricAndPage(String storeName, Tile quadTile, String page, String aggrMethod, String aggrPeriod, String metricId) {
        System.out.println("[getLocalAggregates4MetricAndPage] look in the local store for metric: " + metricId + " (" + storeName + ")");
        final ReadOnlyKeyValueStore<String, TSFragmentData> viewStore = streams.store(storeName,
                QueryableStoreTypes.keyValueStore());
        String quadKey = QuadHash.getQuadKey(quadTile);
        final long fromTimestamp = Instant.parse(page).toEpochMilli();
//        final long toTimestamp = fromTimestamp + getPeriodInterval(AppConfig.LD_FRAGMENT_RES) - getPeriodInterval(aggrPeriod); // to exclude next fragment from the look-up
        final String fromK = quadKey + "#" + toFormattedTimestamp(fromTimestamp, ZoneOffset.UTC);
//        final String toK = quadKey + "#" + toFormattedTimestamp(toTimestamp, ZoneOffset.UTC);
//        System.out.println("fromK=" + fromK);
        TSFragmentData tsFragmentData = viewStore.get(fromK);
        if (tsFragmentData != null) {
            return new ArrayList<>(tsFragmentData.graph.values());
        } else {
            return new ArrayList<>();
        }
    }

//    public static long getPeriodInterval(String timePeriod) {
//        switch (timePeriod) {
//            case "min":
//                return 60000L;
//            case "day":
//                return 86400000L;
//            case "month":
//                return 2592000000L;
//            default:
//                return 3600000L;
//        }
//    }

    public long truncateTS(long timestamp, String resolution) {
        try{
            ZonedDateTime tsDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("UTC"));
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

    private static String toFormattedTimestamp(Long timestamp, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    public boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }
}
