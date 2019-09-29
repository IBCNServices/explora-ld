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

import ingestion.model.AggregateValueTuple;
import ingestion.model.AirQualityReading;
import ingestion.serdes.JsonPOJODeserializer;
import ingestion.serdes.JsonPOJOSerializer;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.commons.lang3.time.DateUtils.truncate;

public class CotIngestStream {

    public static final String DEFAULT_METRIC_ID = "airquality.no2::number";
    public static final String APP_NAME = "cot-aq-ingestion";
    public static final String KBROKERS = "10.10.139.32:9092";
    public static final int DEFAULT_GH_PRECISION = 6;

    public static void main(String[] args) throws Exception {
        String aQMetricId = null;
        int geohashPrecision = 0;
        boolean cleanup = false;

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "m", "metric-id", true, "Air quality Metric ID as registered in Obelisk. Defaults to '" + DEFAULT_METRIC_ID + "'");
        options.addOption( "gh", "geohash-precision", true, "Geohash precision used to perform the continuous aggregation. Defaults to the application " + DEFAULT_GH_PRECISION);
        options.addOption( "cl", "cleanup", false, "Should a cleanup be performed before staring. Defaults to false" );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if( line.hasOption( "metric-id" ) ) {
                aQMetricId = line.getOptionValue("metric-id");
            } else {
                aQMetricId = DEFAULT_METRIC_ID;
            }
            if( line.hasOption( "geohash-precision" ) ) {
                geohashPrecision = Integer.parseInt(line.getOptionValue("geohash-precision"));
            } else {
                geohashPrecision = DEFAULT_GH_PRECISION;
            }
            if( line.hasOption( "cleanup" ) ) {
                cleanup = true;
            }
        }
        catch( Exception exp ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CotIngestStream", exp.getMessage(), options,null, true);
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KBROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        // Set up Serializers and Deserializers

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<AirQualityReading> aQSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AirQualityReading.class);
        aQSerializer.configure(serdeProps, false);

        final Deserializer<AirQualityReading> aQDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AirQualityReading.class);
        aQDeserializer.configure(serdeProps, false);

        final Serde<AirQualityReading> aQSerde = Serdes.serdeFrom(aQSerializer, aQDeserializer);

        serdeProps = new HashMap<>();
        final Serializer<AggregateValueTuple> aggSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AggregateValueTuple.class);
        aggSerializer.configure(serdeProps, false);

        final Deserializer<AggregateValueTuple> aggDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AggregateValueTuple.class);
        aggDeserializer.configure(serdeProps, false);

        final Serde<AggregateValueTuple> aggSerde = Serdes.serdeFrom(aggSerializer, aggDeserializer);

        // Set streaming topology and transformations

        KStream<byte[], AirQualityReading> source = builder.stream("cot.airquality", Consumed.with(Serdes.ByteArray(), aQSerde));
        final String finalMetricId = aQMetricId;
        KStream<String, AirQualityReading> filteredStream = source.selectKey(
                (key, reading) -> reading.getMetricId()
        ).filter(
                (metricId, reading) -> metricId.equals(finalMetricId)
        );
        //.to("cot."+METRIC_ID.replace("::","."), Produced.with(Serdes.String(), aQSerde));
        //.to("cot.airquality-metric-key", Produced.with(Serdes.String(), aQSerde));
        //.print(Printed.toSysOut());

        final int finalGeohashPrecision = geohashPrecision;
        KGroupedStream<String, AirQualityReading> perMinKeyedStream = filteredStream.selectKey(
                (metricId, reading) -> {
                    Date readingDate = new Date(reading.getTimestamp());
                    long minTimestamp = truncate(readingDate, Calendar.MINUTE).getTime();
                    return reading.getGeohash().substring(0, finalGeohashPrecision) + "#" + minTimestamp;
                }
        ).groupByKey();

        KGroupedStream<String, AirQualityReading> perHourKeyedStream = filteredStream.selectKey(
                (metricId, reading) -> {
                    Date readingDate = new Date(reading.getTimestamp());
                    long minTimestamp = truncate(readingDate, Calendar.HOUR).getTime();
                    return reading.getGeohash().substring(0, finalGeohashPrecision) + "#" + minTimestamp;
                }
        ).groupByKey();

        KGroupedStream<String, AirQualityReading> perDayKeyedStream = filteredStream.selectKey(
                (metricId, reading) -> {
                    Date readingDate = new Date(reading.getTimestamp());
                    long minTimestamp = truncate(readingDate, Calendar.DATE).getTime();
                    return reading.getGeohash().substring(0, finalGeohashPrecision) + "#" + minTimestamp;
                }
        ).groupByKey();

        KGroupedStream<String, AirQualityReading> perMonthKeyedStream = filteredStream.selectKey(
                (metricId, reading) -> {
                    Date readingDate = new Date(reading.getTimestamp());
                    long minTimestamp = truncate(readingDate, Calendar.MONTH).getTime();
                    return reading.getGeohash().substring(0, finalGeohashPrecision) + "#" + minTimestamp;
                }
        ).groupByKey();

        KGroupedStream<String, AirQualityReading> perYearKeyedStream = filteredStream.selectKey(
                (metricId, reading) -> {
                    Date readingDate = new Date(reading.getTimestamp());
                    long minTimestamp = truncate(readingDate, Calendar.YEAR).getTime();
                    return reading.getGeohash().substring(0, finalGeohashPrecision) + "#" + minTimestamp;
                }
        ).groupByKey();

        //perMinKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
        //perHourKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
        //perDayKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
        //perMonthKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
        //perYearKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));

        KTable<String, AggregateValueTuple> perMinAggregate = perMinKeyedStream.aggregate(new Initializer<AggregateValueTuple>() {
                                                                                              @Override
                                                                                              public AggregateValueTuple apply() {
                                                                                                  return new AggregateValueTuple(0L, 0.0, 0.0);
                                                                                              }
                                                                                          },
                new Aggregator<String, AirQualityReading, AggregateValueTuple>() {
                    @Override
                    public AggregateValueTuple apply(String key, AirQualityReading value, AggregateValueTuple aggregate) {
                        aggregate.count = aggregate.count + 1;
                        aggregate.sum = aggregate.sum + (Double) value.getValue();
                        aggregate.avg = aggregate.sum / aggregate.count;
                        return aggregate;
                    }
                }, Materialized.<String, AggregateValueTuple, KeyValueStore<Bytes, byte[]>>as("per-min-" + finalMetricId.replace("::", ".")).withValueSerde(aggSerde));

        KStream<String, AggregateValueTuple> perMinAggregateStream = perMinAggregate.toStream();
        perMinAggregateStream.peek((key, aggregate) -> System.out.println(key + ": " + aggregate));

               // .to("per-min-" + aQMetricId.replace("::", ".") + "-output", Produced.with(Serdes.String(), aggSerde));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            if(cleanup) {
                streams.cleanUp();
            }
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}