package querying;

import jdk.management.resource.internal.inst.SocketOutputStreamRMHooks;
import model.AggregateValueTuple;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import util.AppConfig;
import util.HostStoreInfo;
import util.MetadataService;

import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Path("api")
public class CotQueryingService {
    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;
    private final LongSerializer serializer = new LongSerializer();
    private static final Logger log = LoggerFactory.getLogger(CotQueryingService.class);

    CotQueryingService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
    }


    @GET
    @Path("/airquality/{metricId}/aggregate/{aggregate}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<AggregateValueTuple> queryAirQuality(
            @PathParam("metricId") final String metricId,
            @PathParam("aggregate") final String aggregate,
            @Context final UriInfo qParams) {

        // if the specified aggregate operation is not yet supported => 400 Bad Request
        String aggr_op = aggregate.toLowerCase();
        if(!AppConfig.SUPPORTED_AGGR.contains(aggr_op)) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        String geohashes = qParams.getQueryParameters().getOrDefault("geohashes", Collections.singletonList("")).get(0).toLowerCase();
        String source = qParams.getQueryParameters().getOrDefault("src", Collections.singletonList("tiles")).get(0).toLowerCase();
        String resolution = qParams.getQueryParameters().getOrDefault("res", Collections.singletonList("")).get(0).toLowerCase();
        String interval = qParams.getQueryParameters().getOrDefault("interval", Collections.singletonList("")).toString().toLowerCase();
        int geohashPrecision;
        long fromDate, toDate, snap_ts;
        double radius;
        try {
            geohashPrecision = Integer.parseInt(qParams.getQueryParameters().getOrDefault("gh_precision", Collections.singletonList("6")).get(0));
            fromDate = Long.parseLong(qParams.getQueryParameters().getOrDefault("from", Collections.singletonList("-1")).get(0));
            toDate = Long.parseLong(qParams.getQueryParameters().getOrDefault("to", Collections.singletonList("-1")).get(0));
            snap_ts = Long.parseLong(qParams.getQueryParameters().getOrDefault("ts", Collections.singletonList("-1")).get(0));
            radius = Double.parseDouble(qParams.getQueryParameters().getOrDefault("r", Collections.singletonList("-1")).get(0));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        // if the specified geohash precision operation is not yet supported => 400 Bad Request
        if(!AppConfig.SUPPORTED_GH_PRECISION.contains(geohashPrecision)) {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        if (!geohashes.equals("")) {
            if (!(resolution.isEmpty()) && AppConfig.SUPPORTED_RESOLUTIONS.contains(resolution)){
                System.out.println("[query_airquality] query with spatial predicate...");
            } else if (!(interval.isEmpty()) && AppConfig.SUPPORTED_INTERVALS.contains(interval)){
                System.out.println("[query_airquality] query with spatial and time predicates...");
            } else {
                throw new WebApplicationException(Response.Status.BAD_REQUEST);
            }
        } else if (snap_ts != -1){
            System.out.println("[query_airquality] query with time predicate...");
        } else {
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }

        // The genre might be hosted on another instance. We need to find which instance it is on
        // and then perform a remote lookup if necessary.
        final List<HostStoreInfo>
                hosts =
                metadataService.streamsMetadataForStore("view-" + metricId.replace("::", ".") + "-gh" + geohashPrecision + "-" + resolution);

//        // genre is on another instance. call the other instance to fetch the data.
//        if (!thisHost(host)) {
//            return fetchSongPlayCount(host, "kafka-music/charts/genre/" + genre);
//        }
//
//        // genre is on this instance
//        return topFiveSongs(genre.toLowerCase(), KafkaMusicExample.TOP_FIVE_SONGS_BY_GENRE_STORE);

        return null;

    }

    private boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }

}
