-- DDL extraído do Amazon Athena
-- Arquivo gerado automaticamente

CREATE EXTERNAL TABLE `workspace_db.tb_visao_cliente_completa`(
  `pedido_id` string, 
  `cliente_id` string, 
  `valor_total` double, 
  `status_pagamento` string, 
  `transportadora` string, 
  `status_entrega` string, 
  `pagina_acessada` string)
PARTITIONED BY ( 
  `anomesdia` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://itau-self-wkp-us-east-2-903146277540/workspace_db/tb_visao_cliente_completa/data/'
TBLPROPERTIES (
  'auto.purge'='false', 
  'has_encrypted_data'='false', 
  'numFiles'='-1', 
  'parquet.compression'='GZIP', 
  'totalSize'='-1', 
  'transactional'='false', 
  'trino_query_id'='20260318_005947_00106_94wqc', 
  'trino_version'='0.215-24526-g02c3358');
