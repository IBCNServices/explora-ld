# EXPLORA-LD

This is a Kafka Streams implementation of the [EXPLORA framework](https://www.mdpi.com/1424-8220/20/9/2737), which exposes a Linked Data Fragments interface, based on the [AirQualityExpressServer](https://github.com/linkedtimeseries/AirQualityExpressServer).

## Features

* Continuous computation of data summaries, as new data becomes available in [`Obelisk`](https://obelisk.ilabt.imec.be/api/v2/docs/) (Stream processors consume sensor readings from a Kafka topic).

* Three geo-indexing methods: Geohash, Quad-tiles and Slippy tiles (default: quad-tiles).

* Configurable resolution of the spatial fragmentation scheme (default: 14). Multiple resolutions can be specified.

* Data summaries are computed on a custom set of air quality variables supported by Obelisk (default: `airquality.no2::number,airquality.pm10::number`)

* Configurable time-series fragment size (default: 1 hour)

## Architecture

![Explora-LD](./explora-ld-arch.pdf)

## Requirements

* A Kafka topic where the sensor observations from `Obelisk` are being posted. To get data from Obelisk into a Kafka topic you could use [`cot-see-consumer`](https://gitlab.ilabt.imec.be/lordezan/cot-sse-consumer), which is an implementation of a client of the Server-Sent Events endpoint from `Obelisk`.


## Running the server

Use the comands below to run one instance of the server on localhost:7070

``` 
$ docker build explora-ld .

$ docker run --name explora \
	-e METRICS="airquality.no2::number,airquality.pm10::number" \
	-e READINGS_TOPIC="<topic_name>" \
	-e APP_NAME="explora-ingestion" \
	-e KBROKERS="<kafka_host>:<kafka_port>" \
	-e GEO_INDEX="quadtiling" \
	-e PRECISION='14' \
	-e LD_FRAGMENT_RES="hour" \
	-e REST_ENDPOINT_HOSTNAME="0.0.0.0" \
	-e REST_ENDPOINT_PORT="7070" \
	explora-ld
```

## Usage

Once the server is running you can query the data using the following template:

```
http://localhost:7070/data/14/{x}/{y}{?page,aggrMethod,aggrPeriod}
```

Where `x` and `y` are the coordinates of a certain tile and `page` is an ISO date that determines the day the air quality measurements were taken. 
`aggrMethod` and `aggrPeriod` define the used aggregation method and time interval for the aggregation respectively.
 Types for `aggrMethod` currently are `avg`, `sum`, and `count`. Types for `aggrPeriod` are `min`, `hour` and `day`.
 If these two parameters are undefined, the raw data is queried.
For example:

```
http://localhost:7070/data/14/8392/5467?page=2020-05-26T00:00:00.000Z&aggrMethod=avg&aggrPeriod=hour
```


