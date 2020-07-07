package querying.ld;

import jsonld.JSONLDBuilder;
import model.Aggregate;
import model.ErrorMessage;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.AppConfig;
import util.geoindex.Tile;

import javax.servlet.DispatcherType;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

@Path("data")
public class QueryingService {
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final LongSerializer serializer = new LongSerializer();
    private static final Logger log = LoggerFactory.getLogger(QueryingService.class);
    private final QueryingController controller;
    private String lDFragmentResolution;

    public QueryingService(final KafkaStreams streams, final HostInfo hostInfo) {
        this.hostInfo = hostInfo;
        this.controller = new QueryingController(streams, hostInfo);
        this.lDFragmentResolution = System.getenv("LD_FRAGMENT_RES") != null ? System.getenv("LD_FRAGMENT_RES") : "hour";
        if (!AppConfig.SUPPORTED_RESOLUTIONS.contains(this.lDFragmentResolution)) {
            this.lDFragmentResolution = "hour";
        }
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

        Instant pageInstant = Instant.parse(page);
        int pageMins = pageInstant.atZone(ZoneOffset.UTC).getMinute();
        int pageSecs = pageInstant.atZone(ZoneOffset.UTC).getSecond();

        if (pageMins > 0 || pageSecs > 0) {
            Date pageDate = new Date(controller.truncateTS(pageInstant.toEpochMilli(), this.lDFragmentResolution));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String pageDateStr = sdf.format(pageDate);
            String urlRedirect = String.format("/data/%d/%d/%d?page=%s&aggrMethod=%s&aggrPeriod=%s", z, x, y, pageDateStr, aggrMethod, aggrPeriod);
            return Response.seeOther(URI.create(urlRedirect)).build();
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
        return prepareResponse(aggrMethod, aggrPeriod, results, qTile, page, metricId);
    }

    private Response prepareResponse(String aggregate, String aggrPeriod,  TreeMap<String, Aggregate> payload, Tile tile, String page, String metricId){
        long pageLong = Instant.parse(page).toEpochMilli();
        if (metricId.isEmpty()) { // response to client request (not to a request from another stream processor)
            JSONLDBuilder builder = new JSONLDBuilder();
//            Map<String, HashMap> filteredPayload = new LinkedHashMap<>();
////            System.out.println("[prepareResponse] Incoming payload: " + payload);
////            System.out.println("[prepareResponse] Requested aggregate: " + aggregate);
//            for (Map.Entry<String, Aggregate> entry : payload.entrySet()) {
////                System.out.println("[prepareResponse] Inside for loop ...");
//                String key = entry.getKey();
//                Aggregate value = entry.getValue();
//                try {
////                    System.out.println("[prepareResponse] Inside try-catch ...");
//                    HashMap<String, Object> aggrMap = new HashMap<>();
//                    aggrMap.put("value", value.getClass().getField(aggregate).get(value));
//                    aggrMap.put("sensors", value.sensed_by);
//                    filteredPayload.put(key, aggrMap);
////                    System.out.println("[prepareResponse] Filtered payload: " + filteredPayload);
////                        finalResults.put(key, (Double) value.getClass().getField(aggregate).get(value));
//                } catch (NoSuchFieldException | IllegalAccessException ex) {
//                    ex.printStackTrace();
//                    Response errorResp = Response.status(Response.Status.BAD_REQUEST)
//                            .entity(new ErrorMessage(ex.getMessage(), 400))
//                            .build();
//                    throw new WebApplicationException(errorResp);
//                }
//            }
//            LinkedHashMap<String, Object> respMap = builder.buildTile(tile, pageLong, filteredPayload, aggregate, aggrPeriod);
            try {
                LinkedHashMap<String, Object> respMap = builder.buildTile(tile, pageLong, payload, aggregate, aggrPeriod);
                return Response.ok(new GenericEntity<LinkedHashMap<String, Object>>(respMap){}).build();
            } catch (NoSuchFieldException | IllegalAccessException ex) {
                ex.printStackTrace();
                Response errorResp = Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorMessage(ex.getMessage(), 400))
                        .build();
                throw new WebApplicationException(errorResp);
            }
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
        return Response.ok(new GenericEntity<Map<String, Aggregate>>(payload){}).build();
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
        holder.setInitParameter("jersey.config.server.provider.classnames", QueryingService.class.getCanonicalName());
        holder.setInitParameter("cacheControl","public,max-age=31536000");
        FilterHolder filterHolder = context.addFilter(CrossOriginFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
        filterHolder.setInitParameter("allowedOrigins", "*");

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
