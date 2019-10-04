CREATE TABLE view_airquality_no2_number_gh6_min (gh_ts VARCHAR, gh VARCHAR, ts BIGINT, count DOUBLE, sum BIGINT, avg DOUBLE) WITH (kafka_topic='view-airquality.no2.number-gh6-min', value_format='JSON', key='gh_ts', TIMESTAMP = 'ts');
CREATE TABLE view_airquality_no2_number_gh6_hour (gh_ts VARCHAR, gh VARCHAR, ts BIGINT, count DOUBLE, sum BIGINT, avg DOUBLE) WITH (kafka_topic='view-airquality.no2.number-gh6-hour', value_format='JSON', key='gh_ts', TIMESTAMP = 'ts');
CREATE TABLE view_airquality_no2_number_gh6_day (gh_ts VARCHAR, gh VARCHAR, ts BIGINT, count DOUBLE, sum BIGINT, avg DOUBLE) WITH (kafka_topic='view-airquality.no2.number-gh6-day', value_format='JSON', key='gh_ts', TIMESTAMP = 'ts');
CREATE TABLE view_airquality_no2_number_gh6_month (gh_ts VARCHAR, gh VARCHAR, ts BIGINT, count DOUBLE, sum BIGINT, avg DOUBLE) WITH (kafka_topic='view-airquality.no2.number-gh6-month', value_format='JSON', key='gh_ts', TIMESTAMP = 'ts');
CREATE TABLE view_airquality_no2_number_gh6_year (gh_ts VARCHAR, gh VARCHAR, ts BIGINT, count DOUBLE, sum BIGINT, avg DOUBLE) WITH (kafka_topic='view-airquality.no2.number-gh6-year', value_format='JSON', key='gh_ts', TIMESTAMP = 'ts');
CREATE STREAM raw_airquality_no2_number (gh_ts VARCHAR, tsReceivedMs BIGINT, metricId VARCHAR, timestamp BIGINT, sourceId VARCHAR, geohash VARCHAR, h3Index BIGINT, elevation DOUBLE, value DOUBLE, timeUnit VARCHAR) WITH (kafka_topic='raw-airquality.no2.number', value_format='JSON', TIMESTAMP = 'timestamp');

SET 'auto.offset.reset' = 'earliest';
