import sys
from classes.S3Manager import S3Manager
from classes.AthenaManager import AthenaManager
from classes.GlueManager import GlueManager
from classes.genericLogger import GenericLogger
from classes.DataUtils import DataUtils
from classes.Utils import Utils

def s3_init_etl(args: dict) -> dict:
    """Configura infraestrutura S3 e recupera o SQL do Job."""
    s3 = S3Manager(region_name=args['region_name'])
    project_name = f"{args['db']}/{args['table_name']}"
    bucket_name = args.get('bucket_name') or s3.get_bucket_default()

    # 1. Setup de Pastas (Bootstrap)
    structure = s3.setup_project(bucket_name=bucket_name, project_name=project_name)
    
    # 2. Movimentação do SQL para o ambiente do projeto
    s3.copy_file(source_file_uri=args['path_sql_origem'], target_folder_uri=structure['sql'])
    
    # 3. Leitura da Query
    filename = s3.get_filename_from_uri(args['path_sql_origem'])
    query = s3.get_content_sql(bucket=bucket_name, prefix=f"{project_name}/sql", filename=filename)
    
    return {
        "structure": structure,
        "query": query,
        "bucket": bucket_name
    }

def main():
    # --- 1. RESOLUÇÃO DE ARGUMENTOS ---
    # Suporta Glue Params (--JOB_NAME) e Local Env
    required = ['db', 'table_name', 'path_sql_origem', 'region_name', 'partition_name']
    optional = ['reprocessamento', 'range_reprocessamento', 'dia_corte', 'defasagem']
    args = Utils.resolve_args_glue_params(required, optional)

    # --- 2. INICIALIZAÇÃO DE MANAGERS ---
    logger = GenericLogger(name="VigiaOrchestrator", level="INFO")
    s3 = S3Manager(region_name=args['region_name'])
    athena = AthenaManager(region_name=args['region_name'])
    glue = GlueManager(region_name=args['region_name'])

    try:
        # --- 3. BOOTSTRAP S3 ---
        logger.info(f"Iniciando Bootstrap para {args['db']}.{args['table_name']}")
        setup = s3_init_etl(args)

        # --- 4. DEFINIÇÃO DA JANELA TEMPORAL ---
        lista_particoes = DataUtils.generate_partitions(
            p_type=args['partition_name'],
            reprocessamento=str(args.get('reprocessamento', 'False')).lower() == 'true',
            range_reprocessamento=int(args.get('range_reprocessamento', 0)),
            dia_corte=int(args.get('dia_corte')) if args.get('dia_corte') else None,
            defasagem=int(args.get('defasagem', 0))
        )

        if not lista_particoes:
            logger.warning("Nenhuma partição identificada para processamento. Encerrando.")
            return

        # --- 5. ESTRATÉGIA DE EXECUÇÃO ---
        table_exists = glue.table_exists(db=args['db'], table=args['table_name'])

        if table_exists:
            logger.info(f"Modo INCREMENTAL: Processando {len(lista_particoes)} partições.")
            
            for part in lista_particoes:
                logger.info(f" >>> Processando Partição: {part}")
                
                # A. Limpeza Cirúrgica
                s3.clean_partition(
                    s3_uri=setup['structure']['data'],
                    partition_names=args['partition_name'],
                    partition_values=part
                )

                # B. Unload (Gravação) - Note o uso do direct path para evitar erros
                athena.unload_to_s3(
                    sql=setup['query'],
                    target_s3_path=setup['structure']['data'],
                    database=args['db'],
                    temp_s3=setup['structure']['temp'],
                    partition_names=args['partition_name'],
                    sql_params={args['partition_name']: part}
                )

                # C. Registro no Catálogo
                athena.manage_partition(
                    db=args['db'], 
                    table=args['table_name'], 
                    partition_val=part
                )
        
        else:
            logger.info("Modo FIRST_LOAD: Criando tabela via CTAS.")
            # Para o CTAS inicial, usamos a primeira partição da lista ou a atual
            primeira_part = lista_particoes[-1] # Geralmente a mais recente
            
            athena.create_table_as_select(
                sql=setup['query'],
                target_db=args['db'],
                target_table=args['table_name'],
                s3_path_target=setup['structure']['data'],
                temp_s3=setup['structure']['temp'],
                partition_names=args['partition_name'],
                sql_params={args['partition_name']: primeira_part}
            )

        logger.info("Fim do processo 'O Vigia' com sucesso.")

    except Exception as e:
        logger.error(f"Falha Crítica no Job: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()