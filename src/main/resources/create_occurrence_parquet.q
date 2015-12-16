DROP TABLE IF EXISTS occurrence_parquet;

CREATE TABLE occurrence_parquet (
  datasetkey STRING,
  kingdom STRING,
  phylum STRING,
  class_ STRING,
  order_ STRING,
  family STRING,
  genus STRING,
  species STRING,
  lat DOUBLE,
  lng DOUBLE,
  date BIGINT,
  collector STRING
)
STORED AS PARQUET;

insert overwrite table occurrence_parquet
select
  datasetkey,
  kingdom,
  phylum,
  class,
  order_,
  family,
  genus,
  species,
  decimallatitude,
  decimallongitude,
  eventdate,
  recordedby
from occurrence_hdfs;
