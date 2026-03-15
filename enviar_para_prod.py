from pathlib import Path
import re

from classes.S3Manager import *
from classes.genericLogger import GenericLogger

from local_utils.utils import Utils

PASTA_CLASSES = Path("classes")

S3_FOLDER = "Lib/YGGDRA"

logger = GenericLogger(name="YGGDRA", level="INFO")

logger.info("Iniciando envio de código para produção")

for arquivo in PASTA_CLASSES.glob("*.py"):

    conteudo = arquivo.read_text(encoding="utf-8")


    conteudo = re.sub(r"from\s+\.+", "from ", conteudo)
    
    #salva localmente para depuração
    
    Utils.salvar_arquivo(caminho_pasta=S3_FOLDER,conteudo=conteudo, extensao= '.py',nome_arquivo=arquivo.stem)

    logger.info(f"Arquivo enviado para S3: {arquivo.stem}.py")

# faz o deploy da lib no s3 indicado no prefix, no bucket defaul da conta
Utils.deploy_to_s3(local_folder =PASTA_CLASSES, s3_prefix = S3_FOLDER)