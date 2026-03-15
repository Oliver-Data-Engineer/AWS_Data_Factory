from pathlib import Path
import re
from typing import Optional
from classes.S3Manager import S3Manager
from classes.genericLogger import GenericLogger
import os 


class Utils:
    """
    Classe utilitária com funções auxiliares para deploy
    e manipulação de scripts Python.
    """

    @staticmethod
    def _normalize_imports(content: str) -> str:
        """
        Remove imports relativos dos arquivos Python.

        Exemplo:
        from .module import X
        from ..utils import Y

        Resultado:
        from module import X
        from utils import Y
        """

        return re.sub(r"from\s+\.+", "from ", content)
    
    @staticmethod
    def salvar_arquivo(caminho_pasta: str, extensao: str, conteudo: str, nome_arquivo: str):
        """
        Salva um arquivo localmente garantindo a criação da pasta.
        """

        # garante ponto na extensão
        if not extensao.startswith("."):
            extensao = f".{extensao}"

        nome_final = f"{nome_arquivo}{extensao}"
        caminho_completo = os.path.join(caminho_pasta, nome_final)

        os.makedirs(caminho_pasta, exist_ok=True)

        with open(caminho_completo, "w", encoding="utf-8") as arquivo:
            arquivo.write(conteudo)

        return caminho_completo
    


    @staticmethod
    def deploy_to_s3(
        local_folder: str,
        s3_prefix: str,
        bucket_name: Optional[str] = None,
        logger_name: str = "DEPLOY",
    ):
        """
        Realiza o deploy de todos os arquivos `.py` de uma pasta
        local para uma pasta no S3.

        Parameters
        ----------
        local_folder : str
            Caminho da pasta local contendo os arquivos `.py`.

        s3_prefix : str
            Caminho da pasta no S3 onde os arquivos serão enviados.

        bucket_name : Optional[str]
            Nome do bucket S3. Se não for informado, será utilizado
            o bucket padrão configurado no S3Manager.

        logger_name : str
            Nome utilizado para o logger.
        """

        logger = GenericLogger(name=logger_name, level="INFO")
        logger.info("Iniciando deploy da biblioteca para o S3")

        pasta = Path(local_folder)

        # inicializa manager do S3
        s3 = S3Manager(logger_name=logger_name)

        # usa bucket informado ou bucket default
        bucket = bucket_name if bucket_name else s3.get_bucket_default()

        # garante que pasta exista no S3
        s3.create_s3_folder(prefix=s3_prefix)

        for arquivo in pasta.glob("*.py"):

            # leitura do conteúdo
            conteudo = arquivo.read_text(encoding="utf-8")

            # normalização de imports relativos
            conteudo = Utils._normalize_imports(conteudo)

            # envio para o S3
            s3.write_text_file(
                bucket=bucket,
                content=conteudo,
                prefix=s3_prefix,
                filename=arquivo.stem,
                extension=".py",
            )

            logger.info(f"Upload concluído: {arquivo.name}")

        logger.info("Deploy finalizado com sucesso")