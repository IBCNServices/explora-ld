package querying.ld;

import jsonld.JSONLDBuilder;
import jsonld.JSONLDConfig;
import model.Aggregate;
import model.ErrorMessage;
import model.Message;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import querying.ld.QueryingController;
import util.AppConfig;
import util.geoindex.QuadHash;
import util.geoindex.Tile;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Path("data")
public class QueryingService {
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final LongSerializer serializer = new LongSerializer();
    private static final Logger log = LoggerFactory.getLogger(QueryingService.class);
    private final QueryingController controller;

    public QueryingService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.hostInfo = hostInfo;
        this.controller = new QueryingController(streams, hostInfo);
    }

    @GET
    @Path("/{z}/{x}/{y}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAirQualityHistory(
            @PathParam("z") final int z,
            @PathParam("x") final int x,
            @PathParam("y") final int y,
            @Context final UriInfo qParams) {

        // if no page have been provided => 400 Bad Request
        String page = qParams.getQueryParameters().getOrDefault("page", Collections.singletonList("")).get(0).toLowerCase();
        if (page.equals("")) {
            String errorText = "[getAirQualityHistory] You need to provide an ISO date";
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        // if the specified aggregate operation is not yet supported => 400 Bad Request
        String aggrMethod = qParams.getQueryParameters().getOrDefault("aggrMethod", Collections.singletonList("")).get(0).toLowerCase();
        if(!AppConfig.SUPPORTED_AGGR.contains(aggrMethod)) {
            String errorText = String.format("[getAirQualityHistory] aggregate %s is not yet supported", aggrMethod);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }

        String aggrPeriod = qParams.getQueryParameters().getOrDefault("aggrPeriod", Collections.singletonList("")).get(0).toLowerCase();
//        Boolean local = Boolean.valueOf(qParams.getQueryParameters().getOrDefault("local", Collections.singletonList("false")).get(0).toLowerCase());
        String metricId = qParams.getQueryParameters().getOrDefault("metricId", Collections.singletonList("")).get(0).toLowerCase();
        // if the specified precision (z) is not yet supported => 400 Bad Request
        if(!AppConfig.SUPPORTED_PRECISION.contains(z)) {
            String errorText = String.format("[getAirQualityHistory] precision %s is not yet supported", z);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }


        TreeMap<String, Aggregate> results = new TreeMap<>();
        Tile qTile = new Tile(x , y, z);
        if (!(aggrPeriod.isEmpty()) && AppConfig.SUPPORTED_RESOLUTIONS.contains(aggrPeriod)){
//            System.out.println(String.format("[getAirQualityHistory] Querying Explora: %1$s/%2$s/%3$s=%4$s, %5$s, %6$s, %7$s...", z, x, y, searchKey, page, aggrMethod, aggrPeriod));
            results = controller.solveSpatialQuery(qTile, page, aggrMethod, aggrPeriod,  metricId);
        } else {
            String errorText = String.format("[getAirQualityHistory] Invalid value for aggrPeriod (%1$s)", aggrPeriod);
            Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage(errorText, 400))
                    .build();
            System.out.println(errorText);
            throw new WebApplicationException(errorResp);
        }
//        List<String> columns = Arrays.asList("metricId", aggrMethod);
//        HashMap<String, String> metadata = new HashMap<>();
//        metadata.put("variable", "air_quality");
        return prepareResponse(aggrMethod, aggrPeriod, results, qTile, page, metricId, new GenericType<Long>(){});
    }

    private <T> Response prepareResponse(String aggregate, String aggrPeriod,  Map payload, Tile tile, String page, String metricId, GenericType<T> keyType){
        long pageLong = Instant.parse(page).toEpochMilli();
        if (metricId.isEmpty()) { // response to client request (not to a request from another stream processor)
            JSONLDBuilder builder = new JSONLDBuilder();
            HashMap<String, Object> respMap = builder.buildTile(tile, pageLong, payload, aggregate, aggrPeriod);
            return Response.ok(new GenericEntity<Map<String, Object>>(respMap){}).build();
////                Map<Long, Double> finalResults = new TreeMap<>();
//            List data = new ArrayList();
////            System.out.println("[prepareResponse] Incoming payload: " + payload);
//            payload.forEach((key, value) -> {
//                try {
//                    data.add(Arrays.asList(key, value.getClass().getField(aggregate).get(value)));
////                        finalResults.put(key, (Double) value.getClass().getField(aggregate).get(value));
//                } catch (NoSuchFieldException | IllegalAccessException ex) {
//                    ex.printStackTrace();
//                    Response errorResp = Response.status(Response.Status.BAD_REQUEST)
//                            .entity(new ErrorMessage(ex.getMessage(), 400))
//                            .build();
//                    throw new WebApplicationException(errorResp);
//                }
//            });
////            System.out.println("[prepareResponse] Outgoing data: " + data);
//            Message respMessage = new Message(columns, data, metadata);
//            return Response.ok(respMessage).build();
        }
//                System.out.println("[prepareResponse] sending results");
//                System.out.println(results);
        return Response.ok(new GenericEntity<Map<T, Aggregate>>(payload){}).build();
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
