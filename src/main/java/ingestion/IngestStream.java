/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ingestion;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import jsonld.JSONLDConfig;
import model.*;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import querying.ld.QueryingService;
import util.AppConfig;
import util.TSExtractor;
import util.geoindex.QuadHash;
import util.serdes.JsonPOJODeserializer;
import util.serdes.JsonPOJOSerializer;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * This is the entry point of a Kafka Streams application that continuously computes data summaries on the a stream of
 * air quality readings coming from the Bel-Air CoT setup in Antwerp, following the mechanism described in <a href='https://www.mdpi.com/1424-8220/20/9/2737'>this paper</a>.
 *
 * The application supports a number of geo-indexing methods (geohash, quad-tiling, and Slippy tiles) and time
 * resolutions (per-minute, -hour, -day, and -month bins).
 */
public class IngestStream {

    public static final List<String> METRICS = AppConfig.SUPPORTED_METRICS;
    public static final String GEO_INDEX = System.getenv("GEO_INDEX") != null ? System.getenv("GEO_INDEX") : "geohashing";
    public static final String READINGS_TOPIC = System.getenv("READINGS_TOPIC") != null ? System.getenv("READINGS_TOPIC") : "airquality";
    public static final String APP_NAME = System.getenv("APP_NAME") != null ? System.getenv("APP_NAME") : "explora-ingestion";
    public static final String KBROKERS = System.getenv("KBROKERS") != null ? System.getenv("KBROKERS") : "10.10.139.32:9092";
    public static final String REST_ENDPOINT_HOSTNAME = System.getenv("REST_ENDPOINT_HOSTNAME") != null ? System.getenv("REST_ENDPOINT_HOSTNAME") : "localhost";
    public static final int REST_ENDPOINT_PORT = System.getenv("REST_ENDPOINT_PORT") != null ? Integer.parseInt(System.getenv("REST_ENDPOINT_PORT")) : 7070;
    public static final List<Integer> PRECISION_LIST = AppConfig.SUPPORTED_PRECISION;
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS");


    private static Aggregate airQReadingAggregator(String key, AirQualityReading value, Aggregate aggregate) {
//        aggregate.gh_ts = key;
//        aggregate.gh = key.split("#")[0];
//        aggregate.ts = LocalDateTime.parse(key.split("#")[1], DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS")).toInstant(ZoneId.systemDefault().getRules().getOffset(Instant.now())).toEpochMilli();
        aggregate.count = aggregate.count + 1;
        aggregate.sum = aggregate.sum + (Double) value.getValue();
        aggregate.avg = aggregate.sum / aggregate.count;
        aggregate.sensed_by.add(value.getSourceId());
        return aggregate;
    }

    private static TSFragmentData tsFragmentAggregator(String key, AggregateValueTuple value, TSFragmentData tsFragmentAggregate, String aggrPeriod) {
        try {
            LinkedHashMap<String, Object> currentSOSAObservation = buildSOSAObservationPayload(value, aggrPeriod, "avg");
            tsFragmentAggregate.graph.merge(value.ts, currentSOSAObservation,
                    IngestStream::apply);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return tsFragmentAggregate;
    }

//    private static TSFragmentData tsFragmentPerHourAggregator(String key, AggregateValueTuple value, TSFragmentData tsFragmentAggregate) {
//        try {
//            LinkedHashMap<String, Object> currentSOSAObservation = buildSOSAObservationPayload(value, "hour", "avg");
//            tsFragmentAggregate.graph.merge(value.ts, currentSOSAObservation,
//                    IngestStream::apply);
//        } catch (NoSuchFieldException | IllegalAccessException e) {
//            e.printStackTrace();
//        }
//        return tsFragmentAggregate;
//    }
//
//    private static TSFragmentData tsFragmentPerDayAggregator(String key, AggregateValueTuple value, TSFragmentData tsFragmentAggregate) {
//        try {
//            LinkedHashMap<String, Object> currentSOSAObservation = buildSOSAObservationPayload(value, "day", "avg");
//            tsFragmentAggregate.graph.merge(value.ts, currentSOSAObservation,
//                    IngestStream::apply);
//        } catch (NoSuchFieldException | IllegalAccessException e) {
//            e.printStackTrace();
//        }
//        return tsFragmentAggregate;
//    }

    private static LinkedHashMap<String, Object> buildSOSAObservationPayload(AggregateValueTuple value, String aggrPeriod, String aggrMethod) throws NoSuchFieldException, IllegalAccessException {
        String metricId = value.metricId;
        Long timestamp = value.ts;
        LinkedHashMap<String, Object> phenomenonTime = new LinkedHashMap<>();
        LinkedHashMap<String, String> hasBeginning = new LinkedHashMap<>();
        LinkedHashMap<String, String> hasEnd = new LinkedHashMap<>();
        LinkedHashMap<String, Object> outputJSON = new LinkedHashMap<>();
        LinkedHashMap<String, Object> resultJSON = new LinkedHashMap<>();

        hasBeginning.put("inXSDDateTimeStamp", getCurrOrNextDate(timestamp, false, aggrPeriod));
        hasEnd.put("inXSDDateTimeStamp", getCurrOrNextDate(timestamp, true, aggrPeriod));
        phenomenonTime.put("hasBeginning", hasBeginning);
        phenomenonTime.put("hasEnd", hasEnd);

        outputJSON.put("count", value.getClass().getField("count").get(value));
        outputJSON.put("total", value.getClass().getField("sum").get(value));

        resultJSON.put("@id", JSONLDConfig.BASE_URL + metricId + "/" + timestamp);
        resultJSON.put("@type", "sosa:Observation");
        resultJSON.put("hasSimpleResult", value.getClass().getField(aggrMethod).get(value));
        resultJSON.put("resultTime", getCurrOrNextDate(Long.valueOf(timestamp), false, aggrPeriod));
        resultJSON.put("phenomenonTime", phenomenonTime);
        resultJSON.put("observedProperty", JSONLDConfig.BASE_URL + metricId);
        resultJSON.put("madeBySensor", convertSensors((HashSet<String>) value.getClass().getField("sensed_by").get(value)));
        resultJSON.put("usedProcedure", JSONLDConfig.BASE_URL + "id/" + aggrMethod);
        resultJSON.put("hasFeatureOfInterest", JSONLDConfig.BASE_URL + JSONLDConfig.FEATURE_OF_INTEREST);
        resultJSON.put("Output", outputJSON);
        return resultJSON;
    }

    private static LinkedHashMap<String, Object> apply(LinkedHashMap<String, Object> obs1, LinkedHashMap<String, Object> obs2) {
//        System.out.println("OBS1: " + obs1);
//        System.out.println("OBS2: " + obs2);
        return obs2;
//        if (obs1.isEmpty()) {
//            return obs2;
//        } else {
//            LinkedHashMap<String, Object> outputJSON1 = (LinkedHashMap<String, Object>) obs1.get("Output");
//            LinkedHashMap<String, Object> outputJSON2 = (LinkedHashMap<String, Object>) obs2.get("Output");
//            List<String> madeBySensor1 = Collections.unmodifiableList((List<String>) obs1.get("madeBySensor"));
//            List<String> madeBySensor2 = Collections.unmodifiableList((List<String>) obs2.get("madeBySensor"));
//            outputJSON2.forEach((k, v)
//                    -> {
//                outputJSON1.merge(k, v, (v1, v2)
//                        -> (Double.parseDouble(v1.toString()) + Double.parseDouble(v2.toString())));
//            });
//            obs1.put("Output", outputJSON1);
//            obs1.put("hasSimpleResult", ((double) outputJSON1.get("sum") / (double) outputJSON1.get("count")));
//
//            HashSet<String> madeBySensor = new HashSet<>(Stream.concat(madeBySensor1.stream(), madeBySensor2.stream()).collect(Collectors.toSet()));
//            obs1.put("madeBySensor", new ArrayList<>(madeBySensor));
//            return obs1;
//        }
    }

    private static String getCurrOrNextDate(Long page, boolean next, String aggrPeriod){
        Date refPage;
        if (next) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(page);
            switch (aggrPeriod) {
                case "min":
                    cal.add(Calendar.MINUTE, 1);
                    break;
                case "day":
                    cal.add(Calendar.DATE, 1);
                    break;
                case "month":
                    cal.add(Calendar.MONTH, 1);
                    break;
                default:
                    cal.add(Calendar.HOUR, 1);
                    break;
            }
            refPage = cal.getTime();
        } else {
            refPage = new Date(page);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(refPage);
    }

    private static List<String> convertSensors(HashSet<String> sensors) {
        List<String> sensorList = new ArrayList<>();
        for (String sensorId : sensors) {
            sensorList.add(JSONLDConfig.BASE_URL + sensorId);
        }
        return sensorList;
    }

    public static void main(String[] args) throws Exception {
        List<String> aQMetrics = null;
        String endpointHost = null;
        String readingsTopic = null;
        String geoIndex = null;
        int endpointPort = 0;
        List<Integer> precisionList = new ArrayList<>();
        boolean cleanup = false;

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "m", "metric-list", true, "Air quality Metrics as registered in Obelisk. Defaults to '" + METRICS + "'");
        options.addOption( "t", "readings-topic", true, "Topic the air quality metric is being registered to in Obelisk. Defaults to '" + READINGS_TOPIC + "'");
        options.addOption( "gi", "geo-index", true, " Geo-indexing strategy (geohashing or quad-tiling). Defaults to " + GEO_INDEX);
        options.addOption( "gp", "precision", true, "Geohash/Quad-tiles precision used to perform the continuous aggregation. Defaults to the application " + PRECISION_LIST);
        options.addOption( "h", "endpoint-host", true, "REST endpoint hostname. Defaults to " + REST_ENDPOINT_HOSTNAME);
        options.addOption( "p", "endpoint-port", true, "REST endpoint port. Defaults to " + REST_ENDPOINT_PORT);
        options.addOption( "cl", "cleanup", false, "Should a cleanup be performed before staring. Defaults to false" );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if( line.hasOption( "metric-list" ) ) {
                aQMetrics = Stream.of(line.getOptionValue("metric-list").split(",")).collect(Collectors.toList());
            } else {
                aQMetrics = METRICS;
            }
            if( line.hasOption( "readings-topic" ) ) {
                readingsTopic = line.getOptionValue("readings-topic");
            } else {
                readingsTopic = READINGS_TOPIC;
            }
            if( line.hasOption( "geo-index" ) ) {
                geoIndex = line.getOptionValue("geo-index");
                assert AppConfig.SUPPORTED_GEO_INDEXING.contains(geoIndex);
            } else {
                geoIndex = GEO_INDEX;
            }
            if( line.hasOption( "precision" ) ) {
                precisionList = Stream.of(line.getOptionValue("precision").split(",")).map(Integer::parseInt).collect(Collectors.toList());
                AppConfig.SUPPORTED_PRECISION = precisionList;
            } else {
                precisionList = PRECISION_LIST;
            }
            if( line.hasOption( "endpoint-host" ) ) {
                endpointHost = line.getOptionValue("endpoint-host");
            } else {
                endpointHost = REST_ENDPOINT_HOSTNAME;
            }
            if( line.hasOption( "endpoint-port" ) ) {
                endpointPort = Integer.parseInt(line.getOptionValue("endpoint-port"));
            } else {
                endpointPort = REST_ENDPOINT_PORT;
            }
            if( line.hasOption( "cleanup" ) ) {
                cleanup = true;
            }
        }
        catch( Exception exp ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CotIngestStream", exp.getMessage(), options,null, true);
        }

        final HostInfo restEndpoint = new HostInfo(endpointHost, endpointPort);

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + KBROKERS);
        System.out.println("REST endpoint at http://" + endpointHost + ":" + endpointPort);

        final KafkaStreams streams = new KafkaStreams(buildTopology(aQMetrics, readingsTopic, geoIndex, precisionList), streamsConfig("/tmp/airquality"));


        if(cleanup) {
            streams.cleanUp();
        }
        streams.start();
        // Start the Restful proxy for servicing remote access to state stores
        final QueryingService queryingService = startRestProxy(streams, restEndpoint);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                try {
                    streams.close();
                    queryingService.stop();
                } catch (Throwable e) {
                    System.exit(1);
                }
                latch.countDown();
            }
        });

        latch.await();
        System.exit(0);
    }

    private static String toFormattedTimestamp(Long timestamp, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    private static QueryingService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo)
            throws Exception {
        final QueryingService
                interactiveQueriesRestService = new QueryingService(streams, hostInfo);
        interactiveQueriesRestService.start();
        return interactiveQueriesRestService;
    }

    private static Properties streamsConfig(final String stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KBROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, REST_ENDPOINT_HOSTNAME + ":" + REST_ENDPOINT_PORT);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TSExtractor.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024 * 100);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 100);
        return props;
    }

    private static <T> Serde<T> getSerde(Class<T> cls) {
        Map<String, Object> serdeProps = new HashMap<>();
        Serializer<T> serializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", cls);
        serializer.configure(serdeProps, false);

        Deserializer<T> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    private static Topology buildTopology(List<String> aQMetrics, String readingsTopic, String geoIndex, List<Integer> precisionList) {
        final StreamsBuilder builder = new StreamsBuilder();

        // Set up Serializers and Deserializers

        final Serde<AirQualityReading> aQSerde = getSerde(AirQualityReading.class); //Serdes.serdeFrom(aQSerializer, aQDeserializer);
        final Serde<AirQualityKeyedReading> aQKSerde = getSerde(AirQualityKeyedReading.class);
        final Serde<Aggregate> aggSerde = getSerde(Aggregate.class);
        final Serde<AggregateValueTuple> aggVTSerde = getSerde(AggregateValueTuple.class);
        final Serde<TSFragmentData> fragSerde = getSerde(TSFragmentData.class);

        // Set streaming topology and transformations

        assert aQMetrics != null;

        KStream<byte[], AirQualityReading> source = builder.stream(readingsTopic, Consumed.with(Serdes.ByteArray(), aQSerde));
        //final String finalMetricId = aQMetricId;
        KStream<String, AirQualityReading> filteredStream = source.selectKey(
                (key, reading) -> reading.getMetricId()
        ).filter(
                (metricId, reading) -> aQMetrics.contains(metricId)
        );

        System.out.println("AQ Metrics list: " + aQMetrics);
        for (String aQMetricId : aQMetrics) {
            for (Integer precision : precisionList) {
                KGroupedStream<String, AirQualityReading> geoKeyedStream = filteredStream
                        .filter(
                                (metricId, reading) -> metricId.equals(aQMetricId)
                        )
                        .selectKey(
                                (metricId, reading) -> {
//                                    ZonedDateTime readingDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(reading.getTimestamp()), ZoneId.systemDefault());
//                                    String minTimestamp = readingDate.truncatedTo(ChronoUnit.MINUTES).toLocalDateTime().format(DATE_TIME_FORMATTER);
                                    if (geoIndex.equals("quadtiling")) {
                                        LatLong readingCoords = GeoHash.decodeHash(reading.getGeohash());
                                        return QuadHash.getQuadKey(QuadHash.getTile(readingCoords.getLat(), readingCoords.getLon(), precision)); // + "#" + minTimestamp;
                                    } else {
                                        return reading.getGeohash().substring(0, precision); // + "#" + minTimestamp;
                                    }
                                }
                        ).groupByKey();

//                TimeWindows fragmentWindow = AppConfig.LD_FRAGMENT_RES.equals("hour") ? TimeWindows.of(Duration.ofHours(1)) : TimeWindows.of(Duration.ofDays(1));
                KStream<String, AggregateValueTuple> perMinAggregateStream = geoKeyedStream
                        .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                        .aggregate(
                                () -> new Aggregate(0L, 0.0, 0.0, new HashSet<>()),
                                (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                                Materialized.<String, Aggregate, WindowStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + precision + "-min-aux").withValueSerde(aggSerde).withCachingEnabled()
                        ).toStream().map((key, value) -> KeyValue.pair(key.key() + "#" + toFormattedTimestamp(truncateTS(key.window().start(), AppConfig.LD_FRAGMENT_RES), ZoneId.of("UTC")), new AggregateValueTuple(
                                key.key() + "#" + key.window().start(),
                                key.key(),
                                key.window().start(),
                                aQMetricId,
                                value.count,
                                value.sum,
                                value.avg,
                                value.sensed_by
                        )));

                KStream<String, AggregateValueTuple> perHourAggregateStream = geoKeyedStream
                        .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                        .aggregate(
                                () -> new Aggregate(0L, 0.0, 0.0, new HashSet<>()),
                                (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                                Materialized.<String, Aggregate, WindowStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + precision + "-hour-aux").withValueSerde(aggSerde).withCachingEnabled()
                        ).toStream().map((key, value) -> KeyValue.pair(key.key() + "#" + toFormattedTimestamp(truncateTS(key.window().start(), AppConfig.LD_FRAGMENT_RES), ZoneId.of("UTC")), new AggregateValueTuple(
                                key.key() + "#" + key.window().start(),
                                key.key(),
                                key.window().start(),
                                aQMetricId,
                                value.count,
                                value.sum,
                                value.avg,
                                value.sensed_by
                        )));

                KStream<String, AggregateValueTuple> perDayAggregateStream = geoKeyedStream
                        .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                        .aggregate(
                                () -> new Aggregate(0L, 0.0, 0.0, new HashSet<>()),
                                (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                                Materialized.<String, Aggregate, WindowStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + precision + "-day-aux").withValueSerde(aggSerde).withCachingEnabled()
                        ).toStream().map((key, value) -> KeyValue.pair(key.key() + "#" + toFormattedTimestamp(truncateTS(key.window().start(), AppConfig.LD_FRAGMENT_RES), ZoneId.of("UTC")), new AggregateValueTuple(
                                key.key() + "#" + key.window().start(),
                                key.key(),
                                key.window().start(),
                                aQMetricId,
                                value.count,
                                value.sum,
                                value.avg,
                                value.sensed_by
                        )));

                KTable<String, TSFragmentData> perMinAggregate = perMinAggregateStream
                        .groupByKey(Grouped.with(Serdes.String(), aggVTSerde))
                        .aggregate(
                        () -> new TSFragmentData(new TreeMap<>()),
                        (key, value, aggregate) -> tsFragmentAggregator(key, value, aggregate, "min"),
                        Materialized.<String, TSFragmentData, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + precision + "-min").withValueSerde(fragSerde).withCachingEnabled()
                );

                KTable<String, TSFragmentData> perHourAggregate = perHourAggregateStream
                        .groupByKey(Grouped.with(Serdes.String(), aggVTSerde))
                        .aggregate(
                        () -> new TSFragmentData(new TreeMap<>()),
                        (key, value, aggregate) -> tsFragmentAggregator(key, value, aggregate, "hour"),
                        Materialized.<String, TSFragmentData, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + precision + "-hour").withValueSerde(fragSerde).withCachingEnabled()
                );

                KTable<String, TSFragmentData> perDayAggregate = perDayAggregateStream
                        .groupByKey(Grouped.with(Serdes.String(), aggVTSerde))
                        .aggregate(
                        () -> new TSFragmentData(new TreeMap<>()),
                        (key, value, aggregate) -> tsFragmentAggregator(key, value, aggregate, "day"),
                        Materialized.<String, TSFragmentData, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + precision + "-day").withValueSerde(fragSerde).withCachingEnabled()
                );

                // Get streams from KTables to peek into them (to check if they are working as expected)
//                perHourAggregate.toStream().peek((key, aggregate) -> System.out.println("[HOUR] --" + key + ": " + aggregate));
            /*airQualityKTable.toStream().peek((key, reading) -> System.out.println("[RAW] --" + key + ": " + reading));
            perMinAggregate.toStream().peek((key, aggregate) -> System.out.println("[MIN] --" + key + ": " + aggregate));
            perHourAggregate.toStream().peek((key, aggregate) -> System.out.println("[HOUR] --" + key + ": " + aggregate));
            perDayAggregate.toStream().peek((key, aggregate) -> System.out.println("[DAY] --" + key + ": " + aggregate));
            perMonthAggregate.toStream().peek((key, aggregate) -> System.out.println("[MONTH] --" + key + ": " + aggregate));
            perYearAggregate.toStream().peek((key, aggregate) -> System.out.println("[YEAR] --" + key + ": " + aggregate));*/

                // Store KTables as kafka topics (changelog stream)

//            airQualityKeyedStream.to("raw-" + aQMetricId.replace("::", "."), Produced.with(Serdes.String(), aQKSerde));
//            perMinAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-min", Produced.with(Serdes.String(), aggSerde));
//            perHourAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-hour", Produced.with(Serdes.String(), aggSerde));
//            perDayAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-day", Produced.with(Serdes.String(), aggSerde));
//            perMonthAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-month", Produced.with(Serdes.String(), aggSerde));
//            perYearAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-year", Produced.with(Serdes.String(), aggSerde));
            }
        }

        Topology topology = builder.build();
        System.out.println(topology.describe());
        return topology;
    }

    private static long truncateTS(long timestamp, String resolution) {
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
}