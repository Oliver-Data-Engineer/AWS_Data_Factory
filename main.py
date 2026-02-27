from classes.AwsClient import AWSClient
from classes.S3Manager import S3Manager
from classes.GlueManager import GlueManager
from classes.AthenaManager import AthenaManager
from time import sleep
import sys
from typing import Optional


def arquitetura_temp_sql():
    aws = AWSClient(service_name="s3", region_name="us-east-2")
    account_id = aws.account_id
    #criar o bucket
    s3 = S3Manager(region_name="us-east-2")
    sql_bucket_name = 'sql-center-{}'.format(account_id)
    s3.create_bucket(bucket_name=sql_bucket_name)
    print(f"Bucket criado: {sql_bucket_name}")
    # criar pasta sql_center
    s3.create_s3_folder(bucket=sql_bucket_name, prefix='sql_center')
    # criar arquivo sql_consolidacao.sql dentro da pasta sql_center
    s3.write_text_file(bucket=sql_bucket_name, prefix='sql_center', filename='tabela_calendario.sql', content=sql, extension='.sql')
    print("Arquivo SQL criado com sucesso!")                    



def s3_init_etl(args: dict) -> dict:
    """
    Orquestrador de infraestrutura S3 para o processo de ETL.
    
    Retorna:
        dict: {
            "structure": dict,      # URIs de todas as pastas do bootstrap (scripts, data, sql, etc)
            "query": str,           # Conteúdo do arquivo SQL lido
            "sql_uri": str,         # Localização final do arquivo SQL no S3
            "bucket": str           # Nome do bucket utilizado
        }
    """
    # 1. Extração Dinâmica de Parâmetros
    region = args.get('region_name', 'us-east-2')
    table = args.get('table_name', 'default_table')
    db = args.get('db', 'default_db')
    
    # Criamos o nome do projeto baseado na hierarquia de banco/tabela
    project_name = f"{db}/{table}"
    path_sql_origem = args.get('path_sql_origem')
    
    if not path_sql_origem:
        raise ValueError("O parâmetro 'path_sql_origem' é obrigatório para iniciar o ETL.")

    # 2. Inicialização do Manager e Infraestrutura
    s3 = S3Manager(region_name=region)
    bucket_name = args.get('bucket_name') or s3.get_bucket_default()
    
    s3.logger.info(f"Iniciando Setup de S3 para o projeto: {project_name}")
    
    # Garante bucket e estrutura de pastas (Bootstrap)
    # Retorna: {'root': 's3://...', 'scripts': 's3://...', 'sql': 's3://...', ...}
    project_structure = s3.setup_project(bucket_name=bucket_name, project_name=project_name)
    sql_target_folder = project_structure.get('sql')

    # 3. Validação de Origem e Cópia
    if not s3.file_exists(path_sql_origem):
        error_msg = f"Falha crítica: Arquivo de origem não encontrado em {path_sql_origem}"
        s3.logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # Copiamos o SQL do repositório central para a pasta /sql do projeto atual
    s3.logger.info(f"Copiando SQL de origem para a estrutura do projeto...")
    new_sql_uri = s3.copy_file(source_file_uri=path_sql_origem, target_folder_uri=sql_target_folder)

    # 4. Leitura do Conteúdo SQL
    file_name = s3.get_filename_from_uri(new_sql_uri)
    
    # Lemos o SQL da nova localização (já dentro do ambiente do projeto)
    query_content = s3.get_content_sql(
        bucket=bucket_name, 
        prefix=f"{project_name}/sql", 
        filename=file_name
    )

    s3.logger.info(f"Bootstrap S3 concluído para {project_name}.")

    # 5. Retorno Consolidado
    return {
        "structure": project_structure,
        "query": query_content,
        "sql_uri": new_sql_uri,
        "bucket": bucket_name
    }




# arquitetura_temp_sql()
    # Em produção (AWS Glue/Airflow), esses argumentos viriam de sys.argv
    # Exemplo de objeto de argumentos simulado:
# job_args = {
#     'db': 'workspace_db',
#     'table_name': 'tabela_calendario',
#     'path_sql_origem': 's3://sql-center-903146277540/sql_center/tabela_calendario.sql',
#     'region_name': 'us-east-2'
# }

# try:
#     sql_query = s3_init_etl(job_args)
    
#     # A partir daqui, sql_query está pronto para ser injetado no Athena ou Spark
#     print("ETL Pronto para execução da Query.")
    
# except Exception as e:
#     print(f"Erro fatal na inicialização do S3: {e}")
#     sys.exit(1) # Finaliza o Job com erro





if __name__ == "__main__":
    sql = '''
    WITH parametro AS (
    SELECT date_parse('{anomesdia}', '%Y%m%d') AS data_referencia
),

limites AS (
    SELECT
        date_trunc('month', data_referencia)              AS data_inicio,
        date_add(
            'day',
            -1,
            date_add('month', 1, date_trunc('month', data_referencia))
        )                                                 AS data_fim
    FROM parametro
)

SELECT
    d                                           AS data,
    date_format(d, '%Y%m%d')                    AS anomesdia,
    day(d)                                      AS dia,
    week(d)                                     AS semana_ano,
    day_of_week(d)                              AS dia_semana,
    date_format(d, '%W')                        AS nome_dia,
    quarter(d)                                  AS trimestre,
    year(d)                                     AS ano,
    month(d)                                    AS mes
FROM limites
CROSS JOIN UNNEST(
    sequence(data_inicio, data_fim, interval '1' day)
) AS t(d)
ORDER BY data

    '''
    # Simulação de argumentos para teste local
    job_args = {
        'db': 'workspace_db',
        'table_name': 'calendario_mensal',
        'path_sql_origem': 's3://sql-center-903146277540/sql_center/tabela_calendario.sql',
        'region_name': 'us-east-2'
    }

    try:
        glue = GlueManager(region_name=job_args['region_name'])
        athena = AthenaManager(region_name=job_args['region_name'])
        s3 = S3Manager(region_name=job_args['region_name'])
        data_setup = s3_init_etl(args=job_args)
        
        if glue.table_exists(db=job_args['db'], table=job_args['table_name']):
            print("Tabela existe no Glue. Executando query de teste no Athena...")

            s3.clean_partition(
            s3_uri=data_setup['structure']['data'],
            partition_names=["ano", "mes"],
            partition_values=[2023, 1])
            
            # Exemplo de query simples para testar a conexão com o Athena
            athena.unload_to_s3(
                sql=data_setup.get("query", 'SELECT 1'),
                target_s3_path=data_setup['structure']['data'],
                database=job_args['db'],
                temp_s3=data_setup['structure']['temp'],
                partition_names=["ano", "mes"],
                sql_params={"anomesdia": '20230101'}
            )
        else:
            print("Tabela não existe no Glue.")

    except Exception as e:
        print(f"Erro ao verificar tabela no Glue: {e}")