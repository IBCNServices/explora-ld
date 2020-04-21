package querying.ld;

import com.github.davidmoten.geo.Base32;
import com.github.davidmoten.geo.GeoHash;
import model.Aggregate;
import model.AggregateValueTuple;
import model.ErrorMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

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

    public TreeMap<String, Aggregate> solveSpatialQuery(Tile quadTile, String page, String aggrMethod, String aggrPeriod, Boolean local) {
        System.out.println("[solveSpatialQuery] method call");
        List<String> stores = AppConfig.SUPPORTED_METRICS.stream().map(metricId -> {
            return "view-" + metricId.replace("::", ".") + "-gh" + quadTile.getZoom() + "-" + aggrPeriod;
        }).collect(Collectors.toList());

        String quadKey = QuadHash.getQuadKey(quadTile);
        long ts = truncateTS(Instant.parse(page).toEpochMilli(), aggrPeriod);
        String searchKey = quadKey + "#" + toFormattedTimestamp(ts, ZoneId.systemDefault());
        System.out.println("[solveSpatialQuery] ts(truncated)=" + ts);
        System.out.println("[solveSpatialQuery] searchKey=" + searchKey);
        Aggregator<String> aggCollect = AppConfig.SUPPORTED_METRICS.stream().map(metricId -> {
            final String store = "view-" + metricId.replace("::", ".") + "-gh" + quadTile.getZoom() + "-" + aggrPeriod;
            final HostStoreInfo host = metadataService.streamsMetadataForStoreAndKey(store, searchKey, new StringSerializer());
            if (!thisHost(host)) {
                return fetchAggregate(host, quadTile, page, aggrMethod, aggrPeriod);
            } else {
                return getLocalAggregate(store, searchKey, metricId);
            }
        }).collect(Aggregator<String>::new, Aggregator<String>::accept, Aggregator<String>::combine);
        return aggCollect.getAggregateMap();
    }

    public TreeMap<String, Aggregate> fetchAggregate(HostStoreInfo host, Tile quadTile, String page, String aggrMethod, String aggrPeriod) {
        try {
                System.out.println(String.format("[fetchAggregate] Forwarding request to %s:%s", host.getHost(), host.getPort()));
                return client.target(String.format("http://%s:%d/data/%s/%s/%s",
                        host.getHost(),
                        host.getPort(),
                        quadTile.getZoom(),
                        quadTile.getX(),
                        quadTile.getY()))
                        .queryParam("page", page)
                        .queryParam("aggrMethod", aggrMethod)
                        .queryParam("aggrPeriod", aggrPeriod)
                        .queryParam("local", true)
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
    }

    public Map<String, Aggregate> getLocalAggregate(String storeName, String searchKey, String metricId) {
        System.out.println("[getLocalAggregate] Processing request for " + metricId + " with " + searchKey + "...");
        final ReadOnlyKeyValueStore<String, AggregateValueTuple> viewStore = streams.store(storeName,
                QueryableStoreTypes.keyValueStore());
        Map<String, Aggregate> aggregateReadings = new TreeMap<>();
        AggregateValueTuple aggregateVT =  viewStore.get(searchKey);
        Aggregate agg = new Aggregate(aggregateVT.count, aggregateVT.sum, aggregateVT.avg);
        aggregateReadings.merge(metricId, agg,
                (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)));
        return aggregateReadings;
    }

    private Long truncateTS(Long timestamp, String resolution) {
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

    private static String toFormattedTimestamp(Long timestamp, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    public boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }
}
