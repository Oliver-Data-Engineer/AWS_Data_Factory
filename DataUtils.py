from classes.DataUtils import DataUtils
from datetime import datetime

# Simulação de cenário real: Hoje é 27/02/2026
print(f"Data de Referência do Teste: {datetime.now().strftime('%Y-%m-%d')}\n")

testes = [
    {
        "nome": "1. Diário Padrão (D-1)",
        "params": {"p_type": "data", "defasagem": 1}
    },
    {
        "nome": "2. Diário com Reprocessamento (Gatilho Ativo - Hoje é 27)",
        "params": {"p_type": "data", "reprocessamento": True, "range_reprocessamento": 3, "dia_corte": 27}
    },
    {
        "nome": "3. Diário com Reprocessamento (Gatilho Inativo - Corte era dia 26)",
        "params": {"p_type": "data", "reprocessamento": True, "range_reprocessamento": 3, "dia_corte": 26}
    },
    {
        "nome": "4. Mensal com Defasagem (M-1)",
        "params": {"p_type": "anomes", "defasagem": 1}
    },
    {
        "nome": "5. Mensal Fechamento (Gatilho Ativo + Defasagem + Range)",
        "params": {
            "p_type": "anomes", 
            "defasagem": 1, 
            "reprocessamento": True, 
            "range_reprocessamento": 2, 
            "dia_corte": 27
        }
    },
    {
        "nome": "6. Carga Manual Histórica",
        "params": {"p_type": "anomes", "dt_ini": 202506, "dt_fim": 202508}
    }
]

for t in testes:
    print(f"--- Executando: {t['nome']} ---")
    resultado = DataUtils.generate_partitions(**t['params'])
    print(f"Parâmetros: {t['params']}")
    print(f"Resultado: {resultado}\n")