from classes.AthenaManager import AthenaManager
from local_utils.utils import Utils
from classes.genericLogger import GenericLogger
athena = AthenaManager(logger_name=f"YGGDRA")

logger = GenericLogger(name='YGGDRA', level='INFO')

db = 'workspace_db'
table = 'tb_visao_cliente_completa'


# ddl = athena.get_table_ddl(database='workspace_db', table='tb_visao_cliente_completa',temp_s3='s3://itau-self-wkp-us-east-2-903146277540/workspace_db/tb_visao_cliente_completa/temp/')

# print(ddl)


# Utils.salvar_arquivo(conteudo=str(ddl), caminho_pasta="C:/Users/guilh/Documents/GitHub/AWS_Data_Factory/local/sql/"  , nome_arquivo=f'ddl_{table}', extensao=".sql")

# Utils.salvar_ddl_em_arquivo(ddl['ddl'], "C:/Users/guilh/Documents/GitHub/AWS_Data_Factory/local/sql/", f"ddl_{table}")


linhas = athena.count_linhas_particao(database=db, tabela=table, particao={"anomesdia": "20240306"}, output_s3="s3://itau-self-wkp-us-east-2-903146277540/workspace_db/tb_visao_cliente_completa/temp/count/", workgroup="primary")

print(linhas)