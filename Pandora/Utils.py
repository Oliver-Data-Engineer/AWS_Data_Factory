import sys
import os
import sqlglot
from sqlglot import exp
from typing import List, Dict, Optional, Set, Any
from genericLogger import GenericLogger # Mantendo o padrão de logs

class Utils:
    """
    Classe de utilitários para suporte a Jobs Glue e análise de metadados SQL.
    """
    
    # Logger configurado para a classe
    logger = GenericLogger(name="PANDORA.Utils", level="INFO", propagate=True)

    @staticmethod
    def resolve_args_glue_params(required_keys: List[str], optional_keys: Optional[List[str]] = None) -> Dict[str, str]:
        """
        Resolve argumentos do job para Glue ou ambiente local (Docker/Venv).
        
        Args:
            required_keys: Lista de chaves obrigatórias (ex: ['JOB_NAME', 'DB_TARGET'])
            optional_keys: Lista de chaves opcionais.
        """
        optional_keys = optional_keys or []
        all_keys = required_keys + optional_keys
        args_resolved = {}

        try:
            # Tenta resolver via biblioteca nativa do AWS Glue
            from awsglue.utils import getResolvedOptions
            
            # O getResolvedOptions espera os nomes sem '--', mas sys.argv os contém.
            # Ele busca na lista sys.argv os itens que casam com os nomes fornecidos.
            args_resolved = getResolvedOptions(sys.argv, all_keys)
            Utils.logger.info("Argumentos resolvidos via Glue getResolvedOptions.")

        except ImportError:
            # Fallback para variáveis de ambiente (Local/Docker)
            Utils.logger.warning("Ambiente Glue não detectado. Buscando variáveis de ambiente.")
            missing = []
            
            for k in required_keys:
                value = os.environ.get(k)
                if value is None:
                    missing.append(k)
                else:
                    args_resolved[k] = value
            
            for k in optional_keys:
                args_resolved[k] = os.environ.get(k)

            if missing:
                error_msg = f"Argumentos obrigatórios ausentes: {missing}"
                Utils.logger.error(error_msg)
                raise RuntimeError(error_msg)

        return args_resolved

    @staticmethod
    def normalize_identifier(identifier: str) -> Optional[str]:
        """
        Remove aspas, colchetes e converte identificadores SQL para lowercase.
        """
        if not identifier:
            return None
        # Limpa aspas de diversos dialetos (ANSI, Spark, SQL Server)
        return identifier.strip('"[]`').lower()

    @staticmethod
    def get_origens_sql(sql: str, dialect: str = "presto") -> List[Dict[str, str]]:
        """
        Extrai as tabelas de origem de um SQL, ignorando CTEs e tabelas temporárias.
        Utiliza o sqlglot para análise de linhagem.
        
        Args:
            sql: String SQL para análise.
            dialect: Dialeto SQL (Athena usa 'presto' ou 'trino').
        """
        dados_tabelas = []
        vistos = set()

        try:
            # Higienização básica antes do parse
            sql_clean = sql.strip().rstrip(";")
            parsed = sqlglot.parse_one(sql_clean, read=dialect)
        except Exception as e:
            Utils.logger.error(f"Falha ao parsear SQL para extração de origens: {e}")
            return []

        # 1. Identificar todas as CTEs (Common Table Expressions)
        # Elas não devem ser contadas como origens externas.
        ctes_names: Set[str] = set()
        for cte in parsed.find_all(exp.CTE):
            alias = cte.alias
            if alias:
                ctes_names.add(Utils.normalize_identifier(alias))

        # 2. Iterar sobre os nós de tabela no AST (Abstract Syntax Tree)
        for table in parsed.find_all(exp.Table):
            raw_name = table.name
            raw_db = table.db

            clean_name = Utils.normalize_identifier(raw_name)
            clean_db = Utils.normalize_identifier(raw_db)

            if not clean_name:
                continue

            # Filtro: Ignora se for uma referência a uma CTE definida anteriormente
            if clean_name in ctes_names:
                continue
            
            # Identificador único para deduplicação (db.tabela)
            full_identity = f"{clean_db}.{clean_name}" if clean_db else clean_name

            if full_identity not in vistos:
                vistos.add(full_identity)
                
                # Formata a saída para o padrão do GlueManager
                dados_tabelas.append({
                    "name": clean_name,
                    "db": clean_db if clean_db else "-",
                    "path": full_identity
                })

        Utils.logger.info(f"Linhagem SQL: {len(dados_tabelas)} origens externas detectadas.")
        return dados_tabelas