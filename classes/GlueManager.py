import boto3
import time
import copy
from typing import Optional, Dict, List, Any
from .AwsClient import AWSClient
from .genericLogger import GenericLogger
from .Clock import Clock
from .Utils import Utils # Importação para linhagem
from botocore.exceptions import ClientError

class GlueManager(AWSClient):
    """
    Classe responsável por abstrair operações no AWS Glue Data Catalog.
    Agora monitora a performance de busca de metadados e partições.
    """

    def __init__(self, region_name: str = "us-east-2", logger_name: str = "YGGDRA"):
        super().__init__(service_name="glue", region_name=region_name)
        self.logger = GenericLogger(name=f'{logger_name}.Glue', propagate=True)
        self.logger.info(f"GlueManager inicializado na região: {region_name}")

    # --- MÉTODOS DE APOIO ---

    def _sanitize_name(self, name: str) -> str:
        return name.strip().lower() if name else ""

    def parse_partition_value(self, value: str, data_type: str) -> Any:
        data_type = data_type.lower()
        try:
            if not value or value.lower() == 'null':
                return ""
            if 'int' in data_type or 'bigint' in data_type:
                return int(value)
            if 'decimal' in data_type or 'float' in data_type or 'double' in data_type:
                return float(value)
            return value
        except ValueError:
            return value

    # --- VERIFICAÇÕES (Com Medição) ---

    def table_exists(self, db: str, table: str) -> bool:
        """Verifica existência e loga o tempo de resposta do Catálogo."""
        cronometro = Clock()
        cronometro.start()
        
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        try:
            self.client.get_table(DatabaseName=db, Name=table)
            self.logger.debug(f"Verificação de tabela {db}.{table} em {cronometro.stop()}s")
            return True
        except self.client.exceptions.EntityNotFoundException:
            return False
        except Exception as e:
            self.logger.error(f"Erro ao verificar tabela {db}.{table}: {e}")
            raise

    # --- METADADOS (Com Medição) ---

    def get_description_table(self, db: str, table: str) -> Dict:
        """Busca metadados completos da tabela."""
        cronometro = Clock()
        cronometro.start()
        
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        try:
            resp = self.client.get_table(DatabaseName=db, Name=table)
            self.logger.info(f"Descrição de {db}.{table} obtida em {cronometro.stop()}s")
            return resp["Table"]
        except Exception as e:
            self.logger.error(f"Erro ao obter metadados de {db}.{table}: {e}")
            raise

    def get_partition_values(self, db: str, table: str) -> List[Dict]:
        """Varre o catálogo em busca de todas as partições (Suporta Paginação)."""
        cronometro = Clock()
        cronometro.start()
        
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        partitions = []
        try:
            paginator = self.client.get_paginator('get_partitions')
            for page in paginator.paginate(DatabaseName=db, TableName=table):
                partitions.extend(page.get('Partitions', []))
            
            self.logger.info(f"Total de {len(partitions)} partições obtidas para {db}.{table} em {cronometro.formatted}")
            return partitions
        except Exception as e:
            self.logger.error(f"Erro ao listar partições em {cronometro.stop()}s: {e}")
            raise

    def get_last_partition(self, db: str, table: str, partition_keys: List[str], partition_types: List[str]) -> Optional[Dict]:
        """Calcula a última partição com base em ordenação lógica."""
        cronometro = Clock()
        cronometro.start()

        partitions = self.get_partition_values(db, table) # get_partition_values já tem seu clock
        if not partitions:
            return None

        def sort_key(partition):
            values = partition.get('Values', [])
            return [self.parse_partition_value(v, t) for v, t in zip(values, partition_types)]

        try:
            last_partition_raw = sorted(partitions, key=sort_key)[-1]
            values = last_partition_raw.get('Values', [])
            
            self.logger.info(f"Última partição identificada para {db}.{table} em {cronometro.formatted}")
            return dict(zip(partition_keys, values))
        except Exception as e:
            self.logger.error(f"Erro na ordenação lógica após {cronometro.stop()}s: {e}")
            raise

    # --- INTEGRAÇÃO SQL/LINHAGEM (O Coração do Vigia) ---

    def extract_tables_info(self, sql: str) -> List[Dict]:
        """Extrai linhagem e metadados de todas as origens detectadas no SQL."""
        cronometro_total = Clock()
        cronometro_total.start()
        
        self.logger.info("Iniciando varredura de linhagem e metadados...")
        try:
            origens = Utils.get_origens_sql(sql)
            resultados = []

            for origem in origens:
                # Medição individual por tabela de origem
                cron_tab = Clock()
                cron_tab.start()
                
                table = self._sanitize_name(origem['name'])
                db = self._sanitize_name(origem['db'])

                if not self.table_exists(db, table):
                    self.logger.warning(f"Origem {db}.{table} não encontrada no Data Catalog.")
                    continue

                tb_desc = self.get_description_table(db, table)
                p_keys = [p.get("Name") for p in tb_desc.get("PartitionKeys", [])]
                p_types = [p.get("Type") for p in tb_desc.get("PartitionKeys", [])]

                last_part = None
                if p_keys:
                    last_part = self.get_last_partition(db, table, p_keys, p_types)

                resultados.append({
                    "name": table,
                    "db": db,
                    "path": origem.get('path'),
                    "partition_keys": p_keys,
                    "partition_types": p_types,
                    "last_update_partition": last_part,
                    "fetch_time": cron_tab.stop()
                })

            self.logger.info(f"Processo de Linhagem concluído para {len(resultados)} tabelas em {cronometro_total.formatted}")
            return resultados
        except Exception as e:
            self.logger.error(f"Falha na linhagem após {cronometro_total.stop()}s: {e}", exc_info=True)
            raise