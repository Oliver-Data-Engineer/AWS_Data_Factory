import sys
from datetime import datetime
from Yggdra import Utils
from Yggdra.S3Manager import S3Manager
from Yggdra.AthenaManager import AthenaManager
from Yggdra.GlueManager import GlueManager
from Yggdra.genericLogger import GenericLogger
from Yggdra.DataUtils import DataUtils
from Yggdra.ReportManager import ReportManager

# --- CONFIGURAÇÕES E AUXILIARES ---

def s3_init_etl(args: dict, logger_name: str) -> dict:
    """Configura a infraestrutura S3 e prepara o SQL para o Job."""
    s3 = S3Manager(region_name=args['region_name'], logger_name=f"{logger_name}")
    project_name = f"{args['db']}/{args['table_name']}"
    bucket_name = args.get('bucket_name') or s3.get_bucket_default()

    # 1. Setup de Pastas (Bootstrap)
    structure = s3.setup_project(bucket_name=bucket_name, project_name=project_name)
    
    # 2. Movimentação do SQL para o ambiente do projeto
    s3.copy_file(source_file_uri=args['path_sql_origem'], target_folder_uri=structure['sql'])
    
    # 3. Leitura do Conteúdo SQL
    filename = s3.get_filename_from_uri(args['path_sql_origem'])
    query = s3.get_content_sql(bucket=bucket_name, prefix=f"{project_name}/sql", filename=filename)
    
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

# --- CORE ORCHESTRATOR ---

def main():
    # 1. DEFINIÇÃO DOS PARÂMETROS PADRONIZADOS (Uppercase para o Glue)
    # No console do Glue, você usará: --DB, --TABLE_NAME, etc.
    required = ['DB', 'TABLE_NAME', 'PATH_SQL_ORIGEM', 'REGION_NAME', 'PARTITION_NAME']
    optional = ['REPROCESSAMENTO', 'RANGE_REPROCESSAMENTO', 'DIA_CORTE', 'DEFASAGEM', 'LOG_LEVEL', 'BUCKET_NAME', 'JOB_NAME','OWNER']

    # 2. RESOLUÇÃO DINÂMICA
    job_args = Utils.resolve_args_glue_params(required, optional)

    # 2. INICIALIZAÇÃO DO LOGGER RAIZ (Timeline do Produto)
    PRODUCT_NAME = "YGGDRA"
    logger = GenericLogger(name=PRODUCT_NAME, level=job_args['log_level'])
    logger.info(f"🚀 INICIANDO PRODUTO: {PRODUCT_NAME} - FÁBRICA DE SOT")
    logger.info(f'Job Name: {job_args.get("JOB_NAME", "N/A")}')
    logger.info(f'Owner: {job_args.get("OWNER", "N/A")}')


    # Variáveis de controle para o 'finally'
    s3 = None
    data_setup = {}

    try:
        # 3. INSTANCIAÇÃO DOS MANAGERS (Com nomes hierárquicos para propagação de log)
        s3 = S3Manager(region_name=job_args['region_name'], logger_name=f"{PRODUCT_NAME}")
        athena = AthenaManager(region_name=job_args['region_name'], logger_name=f"{PRODUCT_NAME}")
        glue = GlueManager(region_name=job_args['region_name'], logger_name=f"{PRODUCT_NAME}")
        report = ReportManager(job_args)
        
        # 4. SETUP AMBIENTE S3
        data_setup = s3_init_etl(args=job_args, logger_name=PRODUCT_NAME)
        
        # 5. CÁLCULO DA JANELA DE PARTIÇÕES
        lista_particoes = DataUtils.generate_partitions(
            p_type=job_args['partition_name'],
            reprocessamento=job_args.get('REPROCESSAMENTO', False),
            dt_ini=job_args.get('DT_INI', 190001),
            dt_fim = job_args.get('DT_FIM', 190001),
            range_reprocessamento=job_args.get('RANGE_REPROCESSAMENTO', 0),
            defasagem=job_args.get('DEFASAGEM', 0)
        )


        if not lista_particoes:
            logger.warning("Nenhuma partição identificada. Encerrando Job.")
            return

        # 6. ESTRATÉGIA DE CARGA
        if glue.table_exists(db=job_args['db'], table=job_args['table_name']):
            logger.info(f"MODO INCREMENTAL: {len(lista_particoes)} partições detectadas.")
            logger.info(f"Partições a processar: {lista_particoes}")
            
            for part in lista_particoes:
                try:
                    logger.info(f" >>> Processando Partição: {part}")
                    
                    # A. Purge
                    s3.clean_partition(
                        s3_uri=data_setup['structure']['data'],
                        partition_names=job_args['partition_name'],
                        partition_values=part
                    )

                    # B. Unload (Injeção de partição no SQL)
                    resp = athena.unload_to_s3(
                        sql=data_setup['query'],
                        target_s3_path=data_setup['structure']['data'],
                        database=job_args['db'],
                        temp_s3=data_setup['structure']['temp'],
                        partition_names=job_args['partition_name'],
                        sql_params={job_args['partition_name']: part} 
                    )

                    # C. Catalog Sync
                    athena.manage_partition(
                        db=job_args['db'],
                        table=job_args['table_name'],
                        partition_val=part
                    )

                    # Registro de sucesso no report
                    report.add_partition_result(
                        partition=part, 
                        status="Success", 
                        elapsed=resp['elapsed_sec'], 
                        query_id=resp['query_id']
                    )

                except Exception as e:
                    logger.error(f"Falha na partição {part}: {e}")
                    report.add_error(f"Partição {part}", str(e))
        
        else:
            logger.info("MODO FIRST LOAD: Executando CTAS.")
            p_inicial = lista_particoes[-1]
            
            resp_ctas = athena.create_table_as_select(
                sql=data_setup['query'],
                target_db=job_args['db'],
                target_table=job_args['table_name'],
                s3_path_target=data_setup['structure']['data'],
                temp_s3=data_setup['structure']['temp'],
                partition_names=job_args['partition_name'],
                sql_params={job_args['partition_name']: p_inicial}
            )
            report.add_partition_result(p_inicial, "First Load (CTAS)", resp_ctas['elapsed_sec'], resp_ctas['query_id'])

        # 7. GERAÇÃO E PERSISTÊNCIA DO RELATÓRIO MD
        md_report = report.generate_markdown()
        s3.write_text_file(
            bucket=data_setup['bucket'],
            prefix=f"{data_setup['project_path']}/reports",
            filename=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            content=md_report,
            extension="md"
        )
        print("\n" + md_report + "\n")

    except Exception as e:
        logger.critical(f"Falha Crítica no Orquestrador: {e}", exc_info=True)
        if 'report' in locals(): report.add_error("Global", str(e))

    finally:
        # 8. PERSISTÊNCIA DA TIMELINE COMPLETA (BLACK BOX)
        if s3 and 'bucket' in data_setup:
            logger.info("Salvando timeline completa do produto YGGDRA...")
            save_execution_logs(logger, s3, data_setup['bucket'], data_setup['project_path'])
        logger.info("Fim da execução Fábrica de SOT.")

if __name__ == "__main__":
    main()