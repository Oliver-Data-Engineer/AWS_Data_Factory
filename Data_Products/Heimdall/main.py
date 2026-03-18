import json
from datetime import datetime
from classes.S3Manager import S3Manager
from classes.GlueManager import GlueManager
from classes.DataUtils import DataUtils
from classes.genericLogger import GenericLogger

class HeimdallFirewall:
    """
    Guardião da Yggdra. Lê os metadados da tabela alvo, calcula a defasagem 
    de todas as origens e verifica no AWS Glue se os dados estão prontos 
    para o ETL rodar. Se não estiverem, bloqueia a execução.
    """
    def __init__(self, region_name: str, logger: GenericLogger):
        self.region_name = region_name
        self.logger = logger
        self.s3 = S3Manager(region_name=self.region_name, logger_name=self.logger.name)
        self.glue = GlueManager(region_name=self.region_name, logger_name=self.logger.name)

    def _obter_metadados_tabela(self, bucket: str, prefix: str) -> dict:
        """Busca o Gêmeo Digital mais recente no S3 para saber quais são as origens."""
        meta_prefix = f"{prefix}/metadata/"
        arquivos = self.s3.list_files(bucket=bucket, prefix=meta_prefix)
        
        if not arquivos:
            self.logger.warning("Nenhum metadado encontrado. Ignorando validação do firewall (Possível First Load).")
            return {}

        # Pega o último arquivo gerado (ordenação alfabética pelo timestamp)
        ultimo_arquivo = sorted(arquivos)[-1]
        
        # Lê o conteúdo do JSON do S3
        conteudo_json = self.s3.read_text_file(bucket=bucket, key=ultimo_arquivo)
        return json.loads(conteudo_json)

    def validar_prontidao(self, job_args: dict) -> dict:
        """Executa o motor de regras do Firewall."""
        self.logger.info("🛡️ Iniciando Heimdall: Validação de prontidão de origens...")
        
        bucket = job_args.get('bucket_name') or self.s3.get_bucket_default()
        prefix = f"{job_args['db']}/{job_args['table_name']}"
        
        # 1. Obter o mapa de dependências (Lineage)
        metadata = self._obter_metadados_tabela(bucket, prefix)
        upstream_sources = metadata.get("lineage", {}).get("upstream_sources", [])
        
        if not upstream_sources:
            return {"status": "APPROVED", "reason": "Sem origens mapeadas ou First Load."}

        # 2. Quais partições o ETL quer processar agora?
        particoes_alvo = DataUtils.generate_partitions(
            p_type=job_args['partition_name'],
            reprocessamento=job_args.get('reprocessamento', False),
            range_reprocessamento=job_args.get('range_reprocessamento', 0)
        )

        relatorio_auditoria = {
            "timestamp": datetime.now().isoformat(),
            "target_table": f"{job_args['db']}.{job_args['table_name']}",
            "status": "APPROVED",
            "validations": []
        }
        
        bloquear_etl = False

        # 3. Validação Cruzada (Partição Alvo x Origem Defasada)
        for part_alvo in particoes_alvo:
            # Expande a data alvo para conseguir cruzar com origens de granularidades diferentes
            datas_expandidas = DataUtils.expand_date_variables(part_alvo)
            
            for source in upstream_sources:
                db_origem = source.get('database')
                tb_origem = source.get('table')
                part_type_origem = source.get('partition_type')
                part_name_origem = source.get('partition_name')
                defasagem = source.get('defasagem', 0)
                
                # Descobre qual é a data base para essa origem específica
                valor_base = datas_expandidas.get(part_name_origem, part_alvo)
                
                # Calcula a partição que DEVE existir usando a nova função inteligente
                particao_esperada = DataUtils.calcular_defasagem(
                    partition_value=valor_base,
                    partition_type=part_type_origem,
                    defasagem=defasagem
                )
                
                # Checa no AWS Glue se ela fisicamente existe
                # (Assumindo que você adicione ou tenha um check_partition_exists no GlueManager)
                existe = self.glue.check_partition_exists(
                    db=db_origem, 
                    table=tb_origem, 
                    partition_values=[particao_esperada]
                )
                
                check_result = {
                    "target_partition": part_alvo,
                    "origin": f"{db_origem}.{tb_origem}",
                    "expected_partition": particao_esperada,
                    "lag_applied": defasagem,
                    "exists_in_catalog": existe
                }
                relatorio_auditoria["validations"].append(check_result)
                
                if not existe:
                    bloquear_etl = True
                    self.logger.error(f"FALHA: Origem {db_origem}.{tb_origem} não possui a partição {particao_esperada}.")

        # 4. Auditoria e Decisão Final
        relatorio_auditoria["status"] = "BLOCKED" if bloquear_etl else "APPROVED"
        
        # Salva auditoria no S3
        self.s3.write_text_file(
            bucket=bucket,
            prefix=f"{prefix}/audits",
            filename=f"firewall_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            content=json.dumps(relatorio_auditoria, indent=2),
            extension="json"
        )
        
        if bloquear_etl:
            raise Exception("🛡️ HEIMDALL BLOCK: Falta de dados nas tabelas upstream. Abortando ETL para evitar inconsistências.")
            
        self.logger.info("🛡️ Heimdall: Todas as origens validadas. ETL Liberado.")
        return relatorio_auditoria