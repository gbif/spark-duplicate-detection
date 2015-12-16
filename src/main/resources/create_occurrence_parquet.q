drop table if exists occurrence_parquet;

create table occurrence_parquet (
  gbifid INT,
  datasetkey STRING,
  taxonkey INT,
  lat DOUBLE,
  lng DOUBLE,
  date BIGINT,
  collector STRING
)
stored as parquet;

insert overwrite table occurrence_parquet
select
  gbifid,
  datasetkey,
  taxonkey,
  decimallatitude,
  decimallongitude,
  eventdate,
  recordedby
from occurrence_hdfs;
