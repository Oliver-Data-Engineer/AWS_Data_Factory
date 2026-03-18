import sys
from datetime import datetime
from classes.S3Manager import S3Manager
from classes.AthenaManager import AthenaManager
from classes.GlueManager import GlueManager
from classes.genericLogger import GenericLogger
from classes.DataUtils import DataUtils
from classes.ReportManager import ReportManager
from classes.LocalTest import salvar_arquivo
from classes.MetadataManager import MetadataManager
from classes.Clock import Clock
from classes.SourceGuardian import SourceGuardian

def s3_init_etl(args: dict, logger_name: str) -> dict:
    """Configura a infraestrutura S3 e prepara o SQL para o Job."""
    s3 = S3Manager(region_name=args['region_name'], logger_name=f"{logger_name}")
    project_name = f"{args['db']}/{args['table_name']}"
    bucket_name = args.get('bucket_name') or s3.get_bucket_default()

    # 1. Setup de Pastas (Bootstrap)
    structure = s3.setup_project(bucket_name=bucket_name, project_name=project_name)
    
    if args.get('query', None) is None: 
        # 2. Movimentação do SQL para o ambiente do projeto
        s3.copy_file(source_file_uri=args['path_sql_origem'], target_folder_uri=structure['sql'])
        
        # 3. Leitura do Conteúdo SQL
        filename = s3.get_filename_from_uri(args['path_sql_origem'])
        query = s3.get_content_sql(bucket=bucket_name, prefix=f"{project_name}/sql", filename=filename)
    else: 
        query = args.get('query')
    
    return {
        "structure": structure,
        "query": query,
        "bucket": bucket_name,
        "project_path": project_name
    }

def save_execution_logs(logger, s3_manager, bucket, project_path):
    """Coleta o histórico completo do InMemoryHandler e persiste no S3."""
    log_data = logger.get_history_json()
    
    if not log_data or log_data == "[]":
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"log_execution_{timestamp}"
    
    try:
        log_uri = s3_manager.write_text_file(
            bucket=bucket,
            prefix=f"{project_path}/logs",
            filename=log_filename,
            content=log_data,
            extension="json"
        )
        logger.info(f"Timeline completa persistida: {log_uri}")
        return log_uri
    except Exception as e:
        print(f"ERRO CRÍTICO NA GRAVAÇÃO DE LOGS: {e}")
        return None


def main():
    # 1. PARÂMETROS DE EXECUÇÃO ATUALIZADOS
    job_args = {
        'db': 'workspace_db',
        'table_name': 'tb_visao_cliente_completa',
        'path_sql_origem': '', # Ignorado pois vamos passar a query direta
        'region_name': 'us-east-2',
        'partition_name': 'anomesdia',
        'log_level': 'INFO',
        'job_name': 'ETL_Visao_Cliente',
        'owner': 'Guilherme',
        
        # Query atualizada com injeção padrão Python para o formato da Yggdra
        'query': '''
            with base as (
                    SELECT 
                        p.pedido_id,
                        p.cliente_id,
                        p.valor_total,
                        f.status_pagamento,
                        l.transportadora,
                        l.status_entrega,
                        c.pagina_acessada,
                        p.anomesdia
                    FROM workspace_db.tb_pedidos p
                    LEFT JOIN workspace_db.tb_faturamento_cliente f 
                        ON p.cliente_id = f.cliente_id
                    LEFT JOIN workspace_db.tb_logistica l 
                        ON p.pedido_id = l.pedido_id
                    LEFT JOIN workspace_db.tb_clickstream c 
                        ON p.cliente_id = c.cliente_id
                    WHERE 
                        -- Forçando o partition pruning em todas as tabelas
                        cast(p.anomesdia as varchar) = cast('{anomesdia}' as varchar)
                        AND cast(f.anomes as varchar) = cast('{anomes}' as varchar)
                        AND cast(l.data as varchar) = cast('{data}' as varchar)
                        AND cast(c.year as varchar) = cast('{year}' as varchar) AND cast(c.month as varchar) = cast('{month}' as varchar) AND cast(c.day as varchar) = cast('{day}' as varchar)

                    )
                    select * from base
            '''
    }

    # 2. INICIALIZAÇÃO DE CONTROLE
    PRODUCT_NAME = "YGGDRA"
    logger = GenericLogger(name=PRODUCT_NAME, level=job_args['log_level'])
    logger.info(f"🚀 INICIANDO PRODUTO: {PRODUCT_NAME} - DATA FACTORY")
    
    s3 = None
    data_setup = {}
    report = None
    meta_manager = None # Inicializa global para o finally
    execution_timer = Clock()
    execution_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
            # 3. INSTANCIAÇÃO DOS MANAGERS
            s3 = S3Manager(logger_name=f"{PRODUCT_NAME}")
            athena = AthenaManager(region_name=job_args['region_name'], logger_name=f"{PRODUCT_NAME}")
            glue = GlueManager(region_name=job_args['region_name'], logger_name=f"{PRODUCT_NAME}")
            guardian = SourceGuardian(region_name=job_args['region_name'], logger_name=f"{PRODUCT_NAME}")
            
            report = ReportManager(job_args)
            
            # Setup Inicial e Leitura de Query
            data_setup = s3_init_etl(args=job_args, logger_name=PRODUCT_NAME)
            
            # ---------------------------------------------------------
            # 🛡️ AÇÃO DO GUARDIÃO E GERAÇÃO DO GÊMEO DIGITAL (METADATA)
            # ---------------------------------------------------------
            logger.info("Inspecionando linhagem da query e origens no Glue com o Guardião...")
            meta_manager = MetadataManager(job_args)
            meta_manager.register_sql(data_setup['query'])
            meta_manager.register_artifacts(structure=data_setup['structure'])
            
            upstream_lineage = guardian.map_upstream_lineage(query=data_setup['query'])
            
            for source in upstream_lineage:
                # Proteções (get) caso a tabela não seja particionada
                part_types = source.get('partition_types', [])
                part_keys = source.get('partition_keys', [])
                
                meta_manager.add_source(
                    db=source.get('database', 'N/A'),
                    table=source.get('table', 'N/A'),
                    partition_type=part_types[0] if part_types else "N/A",
                    partition_name=part_keys[0] if part_keys else "N/A",
                    partition_value=source.get('last_partition_value'),
                    defasagem=source.get('expected_lag', 1)
                )
            # ---------------------------------------------------------
            
            # 4. CÁLCULO DA JANELA DE PARTIÇÕES
            lista_particoes = DataUtils.generate_partitions(
                p_type=job_args['partition_name'],
                reprocessamento=job_args.get('reprocessamento', False),
                range_reprocessamento=job_args.get('range_reprocessamento', 0)
            )

            if not lista_particoes:
                logger.warning("Nenhuma partição identificada. Encerrando Job.")
                return

            # 5. INÍCIO DO PROCESSAMENTO ETL
            execution_timer.start()
            
            if glue.table_exists(db=job_args['db'], table=job_args['table_name']):
                logger.info(f"MODO INCREMENTAL: {len(lista_particoes)} partições detectadas.")
                
                for part in lista_particoes:
                    try:
                        logger.info(f" >>> Processando Partição: {part}")
                        s3.clean_partition(
                            s3_uri=data_setup['structure']['data'],
                            partition_names=job_args['partition_name'],
                            partition_values=part
                        )

                        
                        parametros_sql = DataUtils.expand_date_variables(part)
                        parametros_sql[job_args['partition_name']] = part
                        
                        resp = athena.unload_to_s3(
                            sql=data_setup['query'],
                            target_s3_path=data_setup['structure']['data'],
                            database=job_args['db'],
                            temp_s3=data_setup['structure']['temp'],
                            partition_names=job_args['partition_name'],
                            sql_params=parametros_sql  # <--- Usando o dicionário expandido
                        )
                        athena.manage_partition(db=job_args['db'], table=job_args['table_name'], partition_val=part)
                        report.add_partition_result(part, "Success", resp['elapsed_sec'], resp['query_id'])
                    except Exception as e:
                        logger.error(f"Falha na partição {part}: {e}")
                        report.add_error(f"Partição {part}", str(e))
            
            else:
                logger.info("MODO FIRST LOAD: Executando CTAS.")
                p_inicial = lista_particoes[-1]
                
                s3.clean_partition(
                    s3_uri=data_setup['structure']['data'],
                    partition_names=job_args['partition_name'],
                    partition_values=p_inicial
                )
                
                
                parametros_sql_ctas = DataUtils.expand_date_variables(p_inicial)
                parametros_sql_ctas[job_args['partition_name']] = p_inicial
                
                resp_ctas = athena.create_table_as_select(
                    sql=data_setup['query'],
                    target_db=job_args['db'],
                    target_table=job_args['table_name'],
                    s3_path_target=data_setup['structure']['data'],
                    temp_s3=data_setup['structure']['temp'],
                    partition_names=job_args['partition_name'],
                    sql_params=parametros_sql_ctas  # <--- Usando o dicionário expandido
                )
                report.add_partition_result(p_inicial, "Success", resp_ctas['elapsed_sec'], resp_ctas['query_id'])

                # Incrementa o DDL original nos metadados
                ddl_info = athena.get_table_ddl(job_args['db'], job_args['table_name'], data_setup['structure']['temp'])
                meta_manager.artifacts["original_ddl"] = ddl_info["ddl"]

            execution_timer.stop()
            logger.info("Lógica de processamento concluída.")

    except Exception as e:
        logger.critical(f"Falha Crítica no Orquestrador: {e}", exc_info=True)
        if report:
            report.add_error("GlobalOrchestrator", str(e))

    finally:
        # A. GRAVAÇÃO DE RELATÓRIOS E LOGS ORIGINAIS
        if report:
            md_report = report.generate_markdown()
            
            if s3 and 'bucket' in data_setup:
                report_name = f"report_{execution_timestamp}"
                s3.write_text_file(
                    bucket=data_setup['bucket'],
                    prefix=f"{data_setup['project_path']}/reports",
                    filename=report_name,
                    content=md_report, extension="md"
                )
            
            # Usando r string para paths no windows evitar o SyntaxError
            salvar_arquivo(
                caminho_pasta=r'C:\Users\guilh\Documents\GitHub\AWS_Data_Factory\local\report',
                extensao='.md', conteudo=md_report, nome_arquivo=f"report_{execution_timestamp}"
            )
            print("\n" + md_report + "\n")

        # B. GRAVAÇÃO DOS METADADOS (GÊMEO DIGITAL) - Agora roda sempre!
        if meta_manager and s3 and 'bucket' in data_setup:
            meta_manager.metrics["total_duration_sec"] = execution_timer.elapsed_seconds
            metadata_json = meta_manager.to_json()
            
            s3.write_text_file(
                bucket=data_setup['bucket'],
                prefix=f"{data_setup['project_path']}/metadata",
                filename=f"metadata_{execution_timestamp}",
                content=metadata_json,
                extension="json"
            )
            
            # Usando r string para paths no windows
            salvar_arquivo(
                caminho_pasta=r'C:\Users\guilh\Documents\GitHub\AWS_Data_Factory\local\metadata',
                extensao='.json', conteudo=metadata_json, nome_arquivo=f"metadata_{execution_timestamp}"
            )

        # C. GRAVAÇÃO TIMELINE
        if s3 and 'bucket' in data_setup:
            logger.info(f"Finalizando timeline do produto {PRODUCT_NAME}...")
            save_execution_logs(logger, s3, data_setup['bucket'], data_setup['project_path'])
        
        logger.info(f"Job finalizado. Tempo de execução: {execution_timer.formatted}.")


if __name__ == "__main__":
    main()