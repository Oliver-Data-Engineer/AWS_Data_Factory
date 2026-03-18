from classes.genericLogger import GenericLogger

from classes.TableProvisioner import TableProvisioner

def provisionar_tabelas():
    # Inicializa o Logger
    logger = GenericLogger(name="Yggdra-Provisioner", level="INFO")
    
    # Define as configurações base para todas as tabelas
    region = 'us-east-2'
    db_name = 'workspace_db'
    
    # Instancia o provisionador uma única vez
    provisioner = TableProvisioner(region_name=region)

    # Lista com as definições das 3 tabelas
    tabelas_para_criar = [
        {
            'db': db_name,
            'table_name': 'tb_pedidos',
            'columns': {
                'pedido_id': 'STRING',
                'cliente_id': 'STRING',
                'valor_total': 'DOUBLE',
                'qtd_itens': 'INT'
            },
            'partition_columns': {
                'anomesdia': 'STRING'
            }
        },
        {
            'db': db_name,
            'table_name': 'tb_faturamento_cliente',
            'columns': {
                'cliente_id': 'STRING',
                'status_pagamento': 'STRING',
                'score_fraude': 'DOUBLE'
            },
            'partition_columns': {
                'anomes': 'STRING'
            }
        },
        {
            'db': db_name,
            'table_name': 'tb_logistica',
            'columns': {
                'entrega_id': 'STRING',
                'pedido_id': 'STRING',
                'transportadora': 'STRING',
                'status_entrega': 'STRING'
            },
            'partition_columns': {
                'data': 'STRING'
            }
        },
            {
            'db': db_name,
            'table_name': 'tb_clickstream',
            'columns': {
                'evento_id': 'STRING',
                'cliente_id': 'STRING',
                'pagina_acessada': 'STRING',
                'tempo_sessao': 'INT'
            },
            'partition_columns': {
                'year': 'STRING',
                'month': 'STRING',
                'day': 'STRING'
            }
        }
    ]

    # Loop de provisionamento
    logger.info(f"Iniciando provisionamento em lote de {len(tabelas_para_criar)} tabelas...")
    
    for config in tabelas_para_criar:
        try:
            logger.info(f"--- Provisionando: {config['table_name']} ---")
            resultado = provisioner.provision(config)
            logger.info(f"Sucesso! Metadados em: {resultado['metadata_uri']}")
        except Exception as e:
            logger.error(f"Falha ao provisionar a tabela {config['table_name']}: {e}")

if __name__ == "__main__":
    provisionar_tabelas()