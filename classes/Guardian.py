from classes.S3Manager import S3Manager
from classes.GlueManager import GlueManager
from classes.Utils import Utils
from typing import Dict, Any, List

class Guardian:
    """
    Produto da Yggdra responsável por inspecionar o código SQL, 
    identificar tabelas upstream (origens) e consultar o AWS Glue 
    para capturar o estado exato das partições no momento da execução.
    """
    def __init__(self, region_name: str, logger_name: str = 'Guardiao'):
        self.glue = GlueManager(logger_name=logger_name, region_name=region_name)
        # S3Manager pode ser injetado ou instanciado se a query precisar ser baixada por ele
        self.s3 = S3Manager(logger_name=logger_name, region_name=region_name) 

    def get_query_from_s3(self, s3_uri: str, bucket_name: str, project_prefix: str) -> str:
        """Busca a query no S3 caso ela não seja passada diretamente."""
        filename = self.s3.get_filename_from_uri(s3_uri)
        return self.s3.get_content_sql(bucket=bucket_name, prefix=f"{project_prefix}/sql", filename=filename)

    def map_upstream_lineage(self, query: str) -> List[Dict[str, Any]]:
        """
        Inspeciona a query e retorna a linhagem estruturada com metadados do Glue.
        """
        origens = Utils.get_origens_sql(query, dialect='presto')
        lineage_details = []

        for origem in origens:
            db = origem.get('db')
            table = origem.get('table')
            path_complete = origem.get('path')

            # Tenta buscar no Glue (Tabelas temporárias ou CTEs podem falhar, por isso o try/except)
            try:
                tb_desc = self.glue.get_description_table(db=db, table=table)
                
                partition_keys = [p.get("Name") for p in tb_desc.get("PartitionKeys", [])]
                partition_types = [p.get("Type") for p in tb_desc.get("PartitionKeys", [])]
                
                last_partition = None
                # Só busca a última partição se a tabela for particionada
                if partition_keys:
                    last_partition = self.glue.get_last_partition(
                        db=db, 
                        table=table,
                        partition_keys=partition_keys,
                        partition_types=partition_types
                    )

                # Estrutura limpa e validada
                source_info = {
                    "path_reference": path_complete,
                    "database": db,
                    "table": table,
                    "partition_keys": partition_keys,
                    "partition_types": partition_types,
                    "last_partition_value": last_partition
                }
                
                lineage_details.append(source_info)
                
            except Exception as e:
                # Se a origem for uma view não materializada ou CTE, loga e pula
                print(f"[Guardiao] Aviso: Não foi possível obter metadados do Glue para {db}.{table}. Erro: {e}")
                
        return lineage_details