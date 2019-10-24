package querying;

import model.Aggregate;
import model.ErrorMessage;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
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
import java.util.*;

@Path("api")
public class CotQueryingService {
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final LongSerializer serializer = new LongSerializer();
    private static final Logger log = LoggerFactory.getLogger(CotQueryingService.class);
    private final CotQuerying controller;

    public CotQueryingService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.hostInfo = hostInfo;
        this.controller = new CotQuerying(streams, hostInfo);
    }

    @GET
    @Path("/airquality/{metricId}/aggregate/{aggregate}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<Long, Aggregate> queryAirQuality(
            @PathParam("metricId") final String metricId,
            @PathParam("aggregate") final String aggregate,
            @Context final UriInfo qParams) {

        // if the specified aggregate operation is not yet supported => 400 Bad Request
        String aggr_op = aggregate.toLowerCase();
        if(!AppConfig.SUPPORTED_AGGR.contains(aggr_op)) {
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(String.format("[queryAirQuality] aggregate %s is not yet supported", aggregate), 400))
                    .build();
            System.out.println(String.format("[queryAirQuality] aggregate %s is not yet supported", aggregate));
            throw new WebApplicationException(errorResp);
        }

        String geohashes = qParams.getQueryParameters().getOrDefault("geohashes", Collections.singletonList("")).get(0).toLowerCase();
        String source = qParams.getQueryParameters().getOrDefault("src", Collections.singletonList("tiles")).get(0).toLowerCase();
        String resolution = qParams.getQueryParameters().getOrDefault("res", Collections.singletonList("")).get(0).toLowerCase();
        String interval = qParams.getQueryParameters().getOrDefault("interval", Collections.singletonList("")).get(0).toLowerCase();
        Boolean local = Boolean.valueOf(qParams.getQueryParameters().getOrDefault("local", Collections.singletonList("false")).get(0).toLowerCase());
        int geohashPrecision;
        long fromDate, toDate, snap_ts;
        try {
            geohashPrecision = Integer.parseInt(qParams.getQueryParameters().getOrDefault("gh_precision", Collections.singletonList("6")).get(0));
            fromDate = Long.parseLong(qParams.getQueryParameters().getOrDefault("from", Collections.singletonList("-1")).get(0));
            toDate = Long.parseLong(qParams.getQueryParameters().getOrDefault("to", Collections.singletonList("-1")).get(0));
            snap_ts = Long.parseLong(qParams.getQueryParameters().getOrDefault("ts", Collections.singletonList("-1")).get(0));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(e.getMessage(), 400))
                    .build();
            throw new WebApplicationException(errorResp);
        }

        // if the specified geohash precision operation is not yet supported => 400 Bad Request
        if(!AppConfig.SUPPORTED_GH_PRECISION.contains(geohashPrecision)) {
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(String.format("[queryAirQuality] geohash precision %s is not yet supported", geohashPrecision), 400))
                    .build();
            System.out.println(String.format("[queryAirQuality] geohash precision %s is not yet supported", geohashPrecision));
            throw new WebApplicationException(errorResp);
        }

        if (!geohashes.equals("")) {
            if (!(resolution.isEmpty()) && AppConfig.SUPPORTED_RESOLUTIONS.contains(resolution)){
                System.out.println("[queryAirQuality] query with spatial predicate...");
                Map<Long, Aggregate> results = controller.solveSpatialQuery(metricId, aggregate, Arrays.asList(geohashes.split(",")), resolution, source, geohashPrecision, local);
//                if (!local) {
//                    Map<Long, Double> finalResults = new TreeMap<>();
//                    results.entrySet()
//                            .forEach(e -> {
//                                try {
//                                    finalResults.put(e.getKey(), (Double) e.getValue().getClass().getField(aggr_op).get(e.getValue()));
//                                } catch (NoSuchFieldException | IllegalAccessException ex) {
//                                    ex.printStackTrace();
//                                    Response errorResp = Response.status(Response.Status.BAD_REQUEST)
//                                            .entity(new ErrorMessage(ex.getMessage(), 400))
//                                            .build();
//                                    throw new WebApplicationException(errorResp);
//                                }
//                            });
//                    return Collections.unmodifiableMap(finalResults);
//                }
                System.out.println("[queryAirQuality] sending results");
                System.out.println(results);
                return results;
            } else if (!(interval.isEmpty()) && AppConfig.SUPPORTED_INTERVALS.contains(interval)){
                System.out.println("[queryAirQuality] query with spatial and time predicates...");
            } else {
                Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorMessage(String.format("[queryAirQuality] Invalid values for resolution (%1$s) or interval (%2$s)", resolution, interval), 400))
                        .build();
                System.out.println(String.format("[queryAirQuality] Invalid values for resolution (%1$s) or interval (%2$s)", resolution, interval));
                throw new WebApplicationException(errorResp);
            }
        } else if (snap_ts != -1){
            System.out.println("[queryAirQuality] query with time predicate...");
        } else {
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("[queryAirQuality] Query parameters are not valid", 400))
                    .build();
            System.out.println("[queryAirQuality] Query parameters are not valid");
            throw new WebApplicationException(errorResp);
        }
        return null;

    }

    /**
     * Start an embedded Jetty Server
     * @throws Exception from jetty
     */
    public void start() throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(hostInfo.host());
        connector.setPort(hostInfo.port());
        jettyServer.addConnector(connector);

        context.start();

        try {
            jettyServer.start();
        } catch (final java.net.SocketException exception) {
            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     * @throws Exception from jetty
     */
    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

}
