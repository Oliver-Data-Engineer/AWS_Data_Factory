import boto3
import time
import copy
from typing import Optional, Dict, List, Any
from .AwsClient import AWSClient
from .GlueManager import GlueManager
from .genericLogger import GenericLogger
from .Clock import Clock

class AthenaManager(AWSClient):
    """
    Classe responsável por orquestrar execuções no Amazon Athena.
    Gerencia consultas assíncronas, operações de UNLOAD e CTAS com medição de performance.
    """

    def __init__(self, region_name: str = "us-east-2",logger_name = 'YGGDRA'):
        super().__init__(service_name="athena", region_name=region_name)
        self.logger = GenericLogger(name=f'{logger_name}.Athena', propagate=True)
        self.glue = GlueManager(region_name=region_name)
        self.region_name = region_name
        self.logger.info(f"AthenaManager inicializado na região: {self.region_name}")

    # --- MÉTODOS PRIVADOS DE SUPORTE ---

    def _wait_for_query(self, query_id: str, timeout: int = 300) -> str:
        """Aguarda a conclusão da query com polling inteligente e cronômetro."""
        cronometro = Clock()
        cronometro.start()
        
        while cronometro.elapsed_seconds < timeout:
            response = self.client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]["State"]
            
            if status == "SUCCEEDED":
                self.logger.info(f"Query {query_id} finalizada em {cronometro.formatted}.")
                return status
            
            if status in ["FAILED", "CANCELLED"]:
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Sem motivo informado.")
                self.logger.error(f"Query {query_id} falhou após {cronometro.formatted}: {reason}")
                raise RuntimeError(f"Athena Query {status}: {reason}")
            
            time.sleep(5)
        
        raise TimeoutError(f"A query {query_id} excedeu o limite de {timeout}s.")

    # --- EXECUÇÃO DE QUERIES (DML / DDL) ---

    def execute_query(self, sql: str, database: str, output_s3: str, workgroup: str = "primary") -> Dict:
        """Executa uma query e retorna um dicionário com ID e tempo decorrido."""
        cronometro = Clock()
        cronometro.start()
        
        clean_output = output_s3 if output_s3.startswith("s3://") else f"s3://{output_s3}"
        
        try:
            resp = self.client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": clean_output},
                WorkGroup=workgroup
            )
            query_id = resp["QueryExecutionId"]
            
            # O wait_for_query já tem seu próprio cronômetro interno para o timeout,
            # mas o cronômetro desta função mede o tempo total da operação.
            self._wait_for_query(query_id)
            cronometro.stop()

            return {
                "status": "Success",
                "query_id": query_id,
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted
            }

        except Exception as e:
            cronometro.stop()
            self.logger.error(f"Erro ao disparar query no Athena após {cronometro.formatted}: {e}")
            raise

    def unload_to_s3(
        self, 
        sql: str, 
        target_s3_path: str, 
        database: str, 
        temp_s3: str, 
        partition_names: list | str, 
        sql_params: Dict[str, Any]
    ) -> Dict:
        """Executa UNLOAD direto na pasta da partição para eficiência incremental."""
        names = [partition_names] if isinstance(partition_names, str) else partition_names
        
        try:
            partition_path = "/".join([f"{n}={sql_params[n]}" for n in names])
        except KeyError as e:
            self.logger.error(f"Parâmetro de partição {e} não encontrado em sql_params.")
            raise

        final_target = f"{target_s3_path.rstrip('/')}/{partition_path}/"
        formatted_inner_sql = sql.format(**sql_params).strip().rstrip(";")

        unload_query = f"""
            UNLOAD ({formatted_inner_sql}) 
            TO '{final_target}' 
            WITH (format = 'PARQUET', compression = 'GZIP')
        """.strip()

        self.logger.info(f"Disparando UNLOAD para: {final_target}")
        # Retorna o dicionário completo do execute_query
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
        """Cria tabela via CTAS com limpeza automática e cronometragem."""
        cronometro = Clock()
        cronometro.start()

        names = [partition_names] if isinstance(partition_names, str) else partition_names
        cols_array = ", ".join([f"'{name}'" for name in names])

        try:
            formatted_sql = sql.format(**sql_params).strip().rstrip(";")
        except KeyError as e:
            self.logger.error(f"Parâmetro {e} ausente para o CTAS.")
            raise

        clean_target = f"{s3_path_target.rstrip('/')}/"
        if overwrite:
            self.execute_query(f"DROP TABLE IF EXISTS {target_db}.{target_table}", target_db, temp_s3)
            
            from .S3Manager import S3Manager
            s3_client = S3Manager(region_name=self.region_name)
            s3_client.delete_prefix(bucket=self._extract_bucket(clean_target), 
                                  prefix=self._extract_prefix(clean_target))

        ctas_query = f"""
            CREATE TABLE {target_db}.{target_table}
            WITH (
                format = 'PARQUET',
                external_location = '{clean_target}',
                partitioned_by = ARRAY[{cols_array}]
            )
            AS {formatted_sql}
        """

        try:
            resp = self.execute_query(ctas_query, target_db, temp_s3)
            cronometro.stop()
            
            self.logger.info(f"CTAS {target_table} concluído em {cronometro.formatted}.")
            
            return {
                "status": "Success",
                "table": f"{target_db}.{target_table}",
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted,
                "query_id": resp["query_id"]
            }
        except Exception as e:
            self.logger.error(f"Falha no CTAS: {e}")
            raise

    # --- MÉTODOS DE UTILITÁRIOS ---

    def _extract_bucket(self, s3_uri: str) -> str:
        return s3_uri.replace("s3://", "").split("/")[0]

    def _extract_prefix(self, s3_uri: str) -> str:
        parts = s3_uri.replace("s3://", "").split("/", 1)
        return parts[1] if len(parts) > 1 else ""

    def manage_partition(self, db: str, table: str, partition_val: Any) -> Dict:
        """Gerencia partições no Glue com medição de tempo de resposta do SDK."""
        cronometro = Clock()
        cronometro.start()
        
        desc = self.glue.get_description_table(db, table)
        new_sd = copy.deepcopy(desc["StorageDescriptor"])
        base_location = new_sd["Location"].rstrip("/")
        new_sd["Location"] = f"{base_location}/anomes={partition_val}/"

        try:
            self.glue.client.create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInput={"Values": [str(partition_val)], "StorageDescriptor": new_sd}
            )
            status = "Created"
        except self.glue.client.exceptions.AlreadyExistsException:
            self.glue.client.update_partition(
                DatabaseName=db,
                TableName=table,
                PartitionValueList=[str(partition_val)],
                PartitionInput={"Values": [str(partition_val)], "StorageDescriptor": new_sd}
            )
            status = "Updated"

        cronometro.stop()
        return {
            "partition": partition_val, 
            "status": status, 
            "elapsed_sec": cronometro.elapsed_seconds
        }

    def get_table_ddl(self, database: str, table: str, temp_s3: str) -> Dict[str, Any]:
        """
        Extrai o DDL (CREATE TABLE statement) de uma tabela existente no Athena.
        Útil para persistência de metadados e linhagem no YGGDRA.
        """
        cronometro = Clock()
        cronometro.start()
        
        query_sql = f"SHOW CREATE TABLE {database}.{table}"
        self.logger.info(f"Extraindo DDL da tabela {database}.{table}")

        try:
            # 1. Executa a query usando o motor padronizado da classe
            # Isso já garante o wait_for_query e o registro do tempo
            exec_resp = self.execute_query(query_sql, database, temp_s3)
            query_id = exec_resp["query_id"]

            # 2. Coleta os resultados da execução
            results = self.client.get_query_results(QueryExecutionId=query_id)
            
            # 3. Processa as linhas do Athena (o DDL vem fragmentado em linhas)
            # Cada linha do 'SHOW CREATE TABLE' vem como uma VarCharValue única
            ddl_lines = [
                row['Data'][0].get('VarCharValue', '') 
                for row in results['ResultSet']['Rows']
            ]
            ddl_final = "\n".join(ddl_lines)

            cronometro.stop()
            self.logger.info(f"DDL extraído com sucesso em {cronometro.formatted}")

            return {
                "status": "Success",
                "database": database,
                "table": table,
                "ddl": ddl_final,
                "query_id": query_id,
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted
            }

        except Exception as e:
            cronometro.stop()
            self.logger.error(f"Falha ao extrair DDL de {database}.{table}: {str(e)}")
            raise