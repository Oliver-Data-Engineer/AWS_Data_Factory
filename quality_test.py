from classes.Utils import Utils


# add_source(self, db: str, table: str, partition_type: str, partition_name: str, partition_value: Any)
from classes.GlueManager import GlueManager

glue = GlueManager(region_name='us-east-2')


sql = '''
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
select * from base'''


origens = Utils.get_origens_sql(sql = sql)
print(origens)
for origem in origens:
    db = origem.get('db',None)
    table = origem.get('table',None)
    conplete_path = origem.get('path')

    print(f"DB: {db} | Table: {table} | Path: {conplete_path}")
    if db and table:
        print(glue.get_description_table(db=db, table=table))
 

print("Teste de qualidade finalizado.") 
