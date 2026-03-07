-- Create Schemas
CREATE SCHEMA IF NOT EXISTS spark_cert_catalog.raw
COMMENT 'Raw ingestion layer';

CREATE SCHEMA IF NOT EXISTS spark_cert_catalog.bronze
COMMENT 'Bronze layer - raw structured ingestion';

CREATE SCHEMA IF NOT EXISTS spark_cert_catalog.silver
COMMENT 'Silver layer - cleaned and validated data';

CREATE SCHEMA IF NOT EXISTS spark_cert_catalog.gold
COMMENT 'Gold layer - business level aggregations';

USE CATALOG spark_cert_catalog;
USE SCHEMA raw;
CREATE VOLUME IF NOT EXISTS raw_data;