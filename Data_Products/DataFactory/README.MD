
# 🌳 YGGRA - Data Factory

> **The Root of Your Data Infrastructure**

O **YGGRA** é um motor de orquestração de dados (Data Factory) desenvolvido para automatizar o ciclo completo de **Extração, Transformação e Carga (ETL)** na AWS. A partir de parâmetros dinâmicos, o YGGRA é capaz de provisionar infraestrutura no S3, gerenciar metadados no Glue Data Catalog e executar processamentos massivos via Amazon Athena.

## 🎯 Responsabilidades do Projeto

A solução foi desenhada para ser o ponto único de verdade (SOT) na criação de pipelines, cuidando de:

* **Bootstrapping de Infraestrutura:** Criação automática de diretórios (`/sql`, `/data`, `/logs`, `/reports`) no S3 seguindo padrões de governança.
* **Orquestração de Carga:** Alternância automática entre `FIRST LOAD` (CTAS) e `INCREMENTAL LOAD` (UNLOAD + Partition Management).
* **Gerenciamento de Metadados:** Sincronização em tempo real com o AWS Glue Data Catalog.

---

## 🛡️ Resiliência e Persistência a Erros

O YGGRA não é apenas um script; é um sistema resiliente. Ele foi construído sob o princípio de **Isolamento de Falha Atômica**:

1. **Processamento por Partição:** Se o pipeline estiver processando 12 meses de dados e o mês 3 falhar, o YGGRA captura o erro, registra no relatório e **continua** o processamento dos meses subsequentes. Isso evita que falhas pontuais interrompam fluxos críticos.
2. **Garantia de Estado (Purge-before-Write):** Antes de qualquer escrita, o YGGRA realiza uma limpeza cirúrgica (Purge) no prefixo S3 de destino, garantindo a idempotência do pipeline (o resultado é o mesmo, não importa quantas vezes você execute).
3. **Bloco `finally` de Segurança:** Independentemente do sucesso ou falha total, o sistema garante a execução da "Caixa Preta" (Black Box) de logs, salvando o estado da memória antes do encerramento do processo.

---

## 📝 Arquitetura de Logs e Observabilidade

A observabilidade é o coração do YGGRA. Diferente de logs convencionais que se perdem no console, o YGGRA implementa uma **Timeline Estruturada**.

### Como o Log é Gerado?

Utilizamos uma hierarquia de loggers onde cada módulo (`S3`, `Athena`, `Glue`) é um "filho" do logger raiz `YGGRA`.

* **Propagação:** Todas as mensagens das sub-classes fluem para o orquestrador principal.
* **InMemoryHandler:** Criamos um buffer em memória que captura cada evento em tempo real, incluindo timestamp, módulo, linha de código e nível de erro.

### Onde e Como são Armazenados?

Ao final de cada execução, o histórico contido na memória é convertido para **JSON estruturado** e persistido automaticamente no S3:

* **Caminho:** `s3://[BUCKET]/[PROJETO]/logs/log_execution_YYYYMMDD_HHMMSS.json`
* **Vantagem:** Por estar em JSON, você pode criar uma tabela no Athena sobre esta pasta e usar o SQL para analisar a performance do seu Data Factory ou identificar padrões de erro entre diferentes tabelas.

---

## ⚙️ Automação via Parâmetros

O YGGRA é agnóstico ao ambiente. Ele utiliza um **Resolvedor Dinâmico de Argumentos** que permite a execução:

1. **Na AWS (Produção):** Lê os parâmetros via `getResolvedOptions` do Glue.
2. **Local/Docker (Desenvolvimento):** Lê variáveis de ambiente (`os.environ`).

### Argumentos de Fábrica (Exemplos):

| Parâmetro | Descrição |
| --- | --- |
| `--DB` | Banco de dados de destino no Glue. |
| `--TABLE_NAME` | Nome da tabela a ser criada/atualizada. |
| `--PATH_SQL_ORIGEM` | URI do S3 contendo a regra de negócio (SQL). |
| `--PARTITION_NAME` | A coluna que define a partição (ex: `anomes`). |
| `--REPROCESSAMENTO` | Flag (`True`/`False`) para ativar carga histórica. |

---

## 📊 Relatório de Execução (The Vigilant)

Além dos logs técnicos, o YGGRA gera um relatório em **Markdown** para consumo humano. Este relatório resume o status de cada partição, o tempo gasto em cada etapa e os IDs de consulta do Athena para auditoria rápida.

> **Status Global:** ✅ SUCESSO
> **Duração Total:** 120s
> **Partições Processadas:** 202601, 202602

---

*YGGRA - Data Factory: Automating the future of data, one branch at a time.*

---
