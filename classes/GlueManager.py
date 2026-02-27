import boto3
from typing import Optional, Dict, List, Any
from .AwsClient import AWSClient
from .genericLogger import GenericLogger
# from .utils import Utils # Supondo que Utils esteja no mesmo pacote
from botocore.exceptions import ClientError

class GlueManager(AWSClient):
    """
    Classe responsável por abstrair operações no AWS Glue Data Catalog.
    Focada em metadados de tabelas e gerenciamento de partições para ETL.
    """

    def __init__(self, region_name: str = "us-east-2"):
        super().__init__(service_name="glue", region_name=region_name)
        self.logger = GenericLogger(name="GlueManager", level="INFO")
        self.logger.info(f"GlueManager inicializado na região: {region_name}")

    # --- MÉTODOS DE APOIO ---

    def _sanitize_name(self, name: str) -> str:
        """Limpa nomes de bancos e tabelas (lowercase e sem espaços)."""
        return name.strip().lower() if name else ""

    def parse_partition_value(self, value: str, data_type: str) -> Any:
        """
        Converte o valor da partição (sempre string no Glue) para o tipo correto 
        para fins de ordenação precisa.
        """
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

    # --- VERIFICAÇÕES ---

    def table_exists(self, db: str, table: str) -> bool:
        """Verifica se uma tabela existe no Data Catalog."""
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        try:
            self.client.get_table(DatabaseName=db, Name=table)
            return True
        except self.client.exceptions.EntityNotFoundException:
            return False
        except Exception as e:
            self.logger.error(f"Erro ao verificar existência da tabela {db}.{table}: {e}")
            raise

    # --- METADADOS ---

    def get_description_table(self, db: str, table: str) -> Dict:
        """Retorna o bloco 'Table' com os metadados do Glue."""
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        self.logger.info(f"Buscando descrição da tabela: {db}.{table}")
        try:
            resp = self.client.get_table(DatabaseName=db, Name=table)
            return resp["Table"]
        except Exception as e:
            self.logger.error(f"Erro ao obter metadados de {db}.{table}: {e}", exc_info=True)
            raise

    def get_partitions_keys(self, db: str, table: str) -> Dict:
        """Retorna o bloco 'Table' com os metadados do Glue."""
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        self.logger.info(f"Buscando descrição da tabela: {db}.{table}")
        try:
            resp = self.client.get_table(DatabaseName=db, Name=table)
            return resp["Table"]['PartitionKeys']
        except Exception as e:
            self.logger.error(f"Erro ao obter metadados de {db}.{table}: {e}", exc_info=True)
            raise

    def get_partition_values(self, db: str, table: str) -> List[Dict]:
        """Retorna todas as partições registradas de uma tabela."""
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        partitions = []
        try:
            # Paginador para garantir que pegamos TODAS as partições (limite do boto3 é 100 por call)
            paginator = self.client.get_paginator('get_partitions')
            for page in paginator.paginate(DatabaseName=db, TableName=table):
                partitions.extend(page.get('Partitions', []))
            
            self.logger.info(f"Total de {len(partitions)} partições obtidas para {db}.{table}")
            return partitions
        except Exception as e:
            self.logger.error(f"Erro ao listar partições de {db}.{table}: {e}")
            raise

    

    def get_last_partition(self, db: str, table: str, partition_keys: List[str], partition_types: List[str]) -> Optional[Dict]:
        """
        Retorna os valores da última partição disponível com base na ordenação lógica.
        """
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)

        partitions = self.get_partition_values(db, table)
        if not partitions:
            return None

        def sort_key(partition):
            values = partition.get('Values', [])
            # Converte valores para tipos reais para que '10' venha depois de '2'
            return [self.parse_partition_value(v, t) for v, t in zip(values, partition_types)]

        try:
            # Ordena e pega a última
            last_partition_raw = sorted(partitions, key=sort_key)[-1]
            values = last_partition_raw.get('Values', [])
            
            # Mapeia Nome da Key -> Valor
            return dict(zip(partition_keys, values))
        except Exception as e:
            self.logger.error(f"Erro ao processar ordenação de partições para {db}.{table}: {e}")
            raise

    # --- INTEGRAÇÃO SQL/LINHAGEM ---

    def extract_tables_info(self, sql: str) -> List[Dict]:
        """
        Analisa um SQL, identifica tabelas de origem e busca metadados no Glue para cada uma.
        Ideal para o projeto 'O Vigia' entender a linhagem dos dados.
        """
        self.logger.info("Iniciando extração de metadados das origens do SQL.")
        try:
            # Utiliza a classe Utils para parsear o SQL (Linhagem)
            origens = Utils.get_origens_sql(sql)
            resultados = []

            for origem in origens:
                table = self._sanitize_name(origem['name'])
                db = self._sanitize_name(origem['db'])
                path = origem.get('path')

                if not self.table_exists(db, table):
                    self.logger.warning(f"Tabela {db}.{table} mencionada no SQL não existe no Glue.")
                    continue

                tb_desc = self.get_description_table(db, table)
                
                # Extrai chaves e tipos de partição
                p_keys = [p.get("Name") for p in tb_desc.get("PartitionKeys", [])]
                p_types = [p.get("Type") for p in tb_desc.get("PartitionKeys", [])]

                last_part = None
                if p_keys:
                    last_part = self.get_last_partition(db, table, p_keys, p_types)

                resultados.append({
                    "name": table,
                    "db": db,
                    "path": path,
                    "partition_keys": p_keys,
                    "partition_types": p_types,
                    "last_update_partition": last_part
                })

            return resultados
        except Exception as e:
            self.logger.error(f"Erro na extração de info das tabelas: {e}", exc_info=True)
            raise