from datetime import datetime
import pandas as pd 

class ReportManager:
    """
    Gerencia a construção de um relatório de execução em Markdown.
    """
    def __init__(self, job_args: dict):
        self.start_time = datetime.now()
        self.job_args = job_args
        self.partitions_results = []
        self.errors = []
        self.summary = {}

    def add_partition_result(self, partition: str, status: str, elapsed: float, query_id: str):
        """Registra o resultado de cada partição do loop."""
        self.partitions_results.append({
            "Partição": f"`{partition}`",
            "Status": "✅ Sucesso" if status == "Success" else "❌ Falha",
            "Tempo (s)": f"{elapsed}s",
            "Query ID": f"[{query_id}](https://console.aws.amazon.com/athena/home#query/history/{query_id})"
        })

    def add_error(self, step: str, message: str):
        """Registra falhas críticas no processo."""
        self.errors.append({"Etapa": step, "Erro": message})

    def generate_markdown(self) -> str:
        """Constrói a string final em Markdown."""
        end_time = datetime.now()
        duration = round((end_time - self.start_time).total_seconds(), 2)
        
        # Cabeçalho e Metadados
        md = f"# 🕒 Relatório de Execução - YGGDRA FÁBRICA DE SOT\n"
        md += f"**Status Global:** {'✅ SUCESSO' if not self.errors else '⚠️ FINALIZADO COM ERROS'}\n\n"
        
        md += "### 📋 Parâmetros do Job\n"
        md += f"- **Tabela:** `{self.job_args['db']}.{self.job_args['table_name']}`\n"
        md += f"- **Tipo Partição:** `{self.job_args['partition_name']}`\n"
        md += f"- **Início:** `{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}`\n"
        md += f"- **Duração Total:** `{duration}s`\n\n"

        # Tabela de Partições (usando Pandas para conversão rápida para MD)
        if self.partitions_results:
            md += "### 🗂️ Detalhes do Processamento\n"
            df = pd.DataFrame(self.partitions_results)
            md += df.to_markdown(index=False)
            md += "\n\n"

        # Seção de Erros
        if self.errors:
            md += "### 🚨 Erros Encontrados\n"
            for err in self.errors:
                md += f"- **{err['Etapa']}:** {err['Erro']}\n"
            md += "\n"

        md += "---\n*Relatório gerado automaticamente pelo motor YGGDRA FÁBRICA DE SOT*"
        return md