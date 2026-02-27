from classes.GlueManager import GlueManager


glue = GlueManager(region_name='us-east-2')

# dados = glue.get_description_table(db='workspace_db', table='calendario_mensal')

glue_description = glue.get_partitions_keys(db='workspace_db', table='calendario_mensal')

print(glue_description)