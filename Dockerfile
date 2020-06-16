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

ENV METRICS 'airquality.no2::number,airquality.pm10::number'
ENV READINGS_TOPIC airquality
ENV APP_NAME explora-ingestion
ENV KBROKERS 10.10.139.32:9092
ENV GEO_INDEX geohashing
ENV PRECISION '6,7'
ENV LD_FRAGMENT_RES hour
ENV REST_ENDPOINT_HOSTNAME 0.0.0.0
ENV REST_ENDPOINT_PORT 7070
ENV ENTRYPOINT_HOST localhost
ENV ENTRYPOINT_PORT 7070


EXPOSE $REST_ENDPOINT_PORT

COPY --from=build /usr/local/service/target/explora-ld-0.1-jar-with-dependencies.jar /usr/local/service/explora-ld-0.1-jar-with-dependencies.jar

CMD ["sh", "-c", "java -cp /usr/local/service/explora-ld-0.1-jar-with-dependencies.jar ingestion.IngestStream --metric-list ${METRIC_ID} --geo-index ${GEO_INDEX} --precision ${PRECISION}"]


