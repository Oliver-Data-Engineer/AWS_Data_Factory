import boto3
import time
import copy
from typing import Optional, Dict, List, Any
from .AwsClient import AWSClient
from .GlueManager import GlueManager
from .genericLogger import GenericLogger

class AthenaManager(AWSClient):
    """
    Classe responsável por orquestrar execuções no Amazon Athena.
    Gerencia consultas assíncronas, operações de UNLOAD e CTAS.
    """

    def __init__(self, region_name: str = "us-east-2"):
        super().__init__(service_name="athena", region_name=region_name)
        self.logger = GenericLogger(name="AthenaManager", level="INFO")
        self.glue = GlueManager(region_name=region_name)
        self.logger.info(f"AthenaManager inicializado na região: {region_name}")

        self.region_name = region_name

    # --- MÉTODOS PRIVADOS DE SUPORTE ---

    def _wait_for_query(self, query_id: str, timeout: int = 300) -> str:
        """Aguarda a conclusão da query com polling inteligente."""
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            response = self.client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]["State"]
            
            if status in ["SUCCEEDED"]:
                self.logger.info(f"Query {query_id} finalizada com sucesso.")
                return status
            
            if status in ["FAILED", "CANCELLED"]:
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Sem motivo.")
                self.logger.error(f"Query {query_id} falhou: {reason}")
                raise RuntimeError(f"Athena Query {status}: {reason}")
            
            time.sleep(5) # Polling de 5 segundos
        
        raise TimeoutError(f"A query {query_id} excedeu o tempo limite de {timeout}s.")

    # --- EXECUÇÃO DE QUERIES (DML / DDL) ---

    def execute_query(self, sql: str, database: str, output_s3: str, workgroup: str = "primary") -> str:
        """Executa uma query simples e retorna o ID da execução."""
        # Normaliza o S3 de saída (Athena exige barra final, mas o S3Manager já cuida disso)
        clean_output = output_s3 if output_s3.startswith("s3://") else f"s3://{output_s3}"
        
        try:
            resp = self.client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": clean_output},
                WorkGroup=workgroup
            )
            query_id = resp["QueryExecutionId"]
            self._wait_for_query(query_id)
            return query_id
        except Exception as e:
            self.logger.error(f"Erro ao disparar query no Athena: {e}")
            raise


    def unload_to_s3(
        self, 
        sql: str, 
        target_s3_path: str, 
        database: str, 
        temp_s3: str, 
        partition_names: list | str, 
        sql_params: Dict[str, Any]
    ) -> str:
        """
        Versão 'Incremental Proof': Escreve diretamente na pasta da partição,
        evitando o erro HIVE_PATH_ALREADY_EXISTS no diretório raiz.
        """
        # 1. Normalização dos nomes
        names = [partition_names] if isinstance(partition_names, str) else partition_names
        
        # 2. Construção do PATH FINAL (Leaf Path)
        # Pegamos os valores do dicionário sql_params na ordem dos nomes das partições
        try:
            partition_path = "/".join([f"{n}={sql_params[n]}" for n in names])
        except KeyError as e:
            self.logger.error(f"O parâmetro de partição {e} não foi encontrado em sql_params.")
            raise

        # O destino agora é a pasta específica, ex: .../data/ano=2023/mes=1/
        # Como limpamos essa pasta antes, o Athena não vai reclamar!
        final_target = f"{target_s3_path.rstrip('/')}/{partition_path}/"

        # 3. Preparação do SQL (Removendo o ; se existir)
        formatted_inner_sql = sql.format(**sql_params).strip().rstrip(";")

        # 4. Construção do UNLOAD SEM partitioned_by
        # Note que removemos o 'partitioned_by' pois o S3 já está no local correto.
        unload_query = f"""
            UNLOAD ({formatted_inner_sql}) 
            TO '{final_target}' 
            WITH (
                format = 'PARQUET', 
                compression = 'GZIP'
            )
        """.strip()

        self.logger.info(f"Disparando UNLOAD DIRETO para a pasta: {final_target}")
        return self.execute_query(unload_query, database, temp_s3)
    
    def create_table_as_select(
        self, 
        target_db: str, 
        target_table: str, 
        sql: str, 
        s3_path_target: str, 
        temp_s3: str, 
        partition_names: list | str, 
        sql_params: Dict[str, Any],
        overwrite: bool = True
    ) -> Dict:
        """
        Cria uma tabela via CTAS (Create Table As Select) no Athena.
        Realiza limpeza automática do S3 para evitar HIVE_PATH_ALREADY_EXISTS.
        """
        self.logger.info(f"Iniciando processo CTAS para {target_db}.{target_table}")
        start_time = time.time()

        # 1. Normalização de Partições
        names = [partition_names] if isinstance(partition_names, str) else partition_names
        cols_array = ", ".join([f"'{name}'" for name in names])

        # 2. Resolução do SQL e Limpeza de Semicolon
        try:
            formatted_sql = sql.format(**sql_params).strip().rstrip(";")
        except KeyError as e:
            self.logger.error(f"Parâmetro {e} ausente no dicionário sql_params.")
            raise

        # 3. Gestão de Overwrite (Drop + S3 Purge)
        clean_target = f"{s3_path_target.rstrip('/')}/"
        if overwrite:
            self.logger.info(f"Overwrite habilitado. Preparando terreno em {clean_target}")
            
            # Drop da tabela no Glue
            self.execute_query(f"DROP TABLE IF EXISTS {target_db}.{target_table}", target_db, temp_s3)
            
            # Limpeza física do S3 (Fundamental para evitar erro de diretório já existente)
            # Aqui usamos o delete_prefix que criamos no S3Manager
            # Assumindo que s3_manager está disponível ou injetado
            from .S3Manager import S3Manager
            s3_client = S3Manager(region_name=self.region_name)
            s3_client.delete_prefix(bucket=self._extract_bucket(clean_target), 
                                  prefix=self._extract_prefix(clean_target))

        # 4. Construção da Query CTAS
        ctas_query = f"""
            CREATE TABLE {target_db}.{target_table}
            WITH (
                format = 'PARQUET',
                external_location = '{clean_target}',
                partitioned_by = ARRAY[{cols_array}]
            )
            AS {formatted_sql}
        """

        self.logger.debug(f"Executando CTAS Query:\n{ctas_query}")
        
        try:
            query_id = self.execute_query(ctas_query, target_db, temp_s3)
            elapsed = round(time.time() - start_time, 2)
            
            self.logger.info(f"Tabela {target_db}.{target_table} criada com sucesso em {elapsed}s.")
            
            return {
                "status": "Success",
                "table": f"{target_db}.{target_table}",
                "location": clean_target,
                "elapsed_sec": elapsed,
                "query_id": query_id
            }
        except Exception as e:
            self.logger.error(f"Falha na execução do CTAS: {e}")
            raise

    def _extract_bucket(self, s3_uri: str) -> str:
        return s3_uri.replace("s3://", "").split("/")[0]

    def _extract_prefix(self, s3_uri: str) -> str:
        parts = s3_uri.replace("s3://", "").split("/", 1)
        return parts[1] if len(parts) > 1 else ""

    # --- GESTÃO DE PARTIÇÕES (Integração Glue) ---

    def repair_table(self, database: str, table: str, temp_s3: str):
        """Sincroniza partições do S3 com o Data Catalog (MSCK REPAIR)."""
        self.logger.info(f"Sincronizando partições de {database}.{table}")
        return self.execute_query(f"MSCK REPAIR TABLE {table}", database, temp_s3)

    def manage_partition(self, db: str, table: str, partition_val: int) -> Dict:
        """
        Cria ou atualiza uma partição no Glue usando metadados da própria tabela.
        """
        self.logger.info(f"Gerenciando partição anomes={partition_val} em {db}.{table}")
        
        # Busca metadados da tabela via GlueManager
        desc = self.glue.get_description_table(db, table)
        
        # Deep copy do StorageDescriptor para evitar mutação de estado
        new_sd = copy.deepcopy(desc["StorageDescriptor"])
        base_location = new_sd["Location"].rstrip("/")
        new_sd["Location"] = f"{base_location}/anomes={partition_val}/"

        try:
            self.glue.client.create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInput={
                    "Values": [str(partition_val)],
                    "StorageDescriptor": new_sd
                }
            )
            status = "Created"
        except self.glue.client.exceptions.AlreadyExistsException:
            self.glue.client.update_partition(
                DatabaseName=db,
                TableName=table,
                PartitionValueList=[str(partition_val)],
                PartitionInput={
                    "Values": [str(partition_val)],
                    "StorageDescriptor": new_sd
                }
            )
            status = "Updated"

        return {"partition": partition_val, "status": status}