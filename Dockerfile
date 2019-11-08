#
# build
#
FROM maven:3.6.2-jdk-8-slim AS build

MAINTAINER Leandro Ordonez Ante (leandro.ordonez.ante@gmail.com)

#RUN mkdir /usr/share/man/man1
RUN apt-get update
RUN apt-get install -y htop

COPY pom.xml /usr/local/service/pom.xml
COPY src /usr/local/service/src

RUN mvn -f /usr/local/service/pom.xml compile assembly:single

#
# Package stage
#
FROM openjdk:14-ea-15-jdk-slim

ENV METRIC_ID airquality.no2::number
ENV READINGS_TOPIC cot.airquality
ENV APP_NAME cot-aq-ingestion
ENV KBROKERS 10.10.139.32:9092
ENV GEOHASH_PRECISION '5,6,7'
ENV REST_ENDPOINT_HOSTNAME 0.0.0.0
ENV REST_ENDPOINT_PORT 7070

EXPOSE $REST_ENDPOINT_PORT

COPY --from=build /usr/local/service/target/cot-aqa-kafka-0.1-jar-with-dependencies.jar /usr/local/service/cot-aqa-kafka-0.1-jar-with-dependencies.jar

CMD ["sh", "-c", "java -cp /usr/local/service/cot-aqa-kafka-0.1-jar-with-dependencies.jar ingestion.CotIngestStream --metric-id ${METRIC_ID} --geohash-precision ${GEOHASH_PRECISION}"]


