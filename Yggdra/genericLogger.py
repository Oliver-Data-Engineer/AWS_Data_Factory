import logging
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional

class InMemoryHandler(logging.Handler):
    """Handler customizado para armazenar logs em memória como dicionários Python."""
    
    def __init__(self):
        super().__init__()
        self.records: List[Dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                # datetime.now(timezone.utc) é a forma moderna/recomendada em Python 3.x
                "timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds'),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module
            }
            self.records.append(entry)
        except Exception:
            self.handleError(record) # Método padrão do logging para falhas no handler

    def to_json(self, indent: Optional[int] = 2) -> str:
        return json.dumps(self.records, ensure_ascii=False, indent=indent)

    def clear(self) -> None:
        self.records.clear()


class GenericLogger:
    """
    Abstração de log com suporte a múltiplas saídas e persistência em memória.
    Perfeito para auditoria de processos ETL.
    """

    _LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __init__(
        self, 
        name: str = "PANDORA", 
        level: str = "INFO", 
        propagate: bool = True,
        to_file: Optional[str] = None  # Parâmetro adicionado para evitar o TypeError
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self._LEVELS.get(level.upper(), logging.INFO))
        self.logger.propagate = propagate 

        # Passamos o to_file para o setup
        self.memory_handler = self._setup_handlers(to_file)

    def _setup_handlers(self, to_file: Optional[str] = None) -> InMemoryHandler:
        """Configura handlers de forma inteligente para evitar duplicidade."""
        
        # 1. Se já existir um InMemoryHandler, não faz nada (Idempotência)
        for handler in self.logger.handlers:
            if isinstance(handler, InMemoryHandler):
                return handler

        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

        # --- A LÓGICA ANTI-DUPLICIDADE ---
        # Só adicionamos saída de Console/Arquivo se:
        # A) For o logger raiz (não tem ponto no nome: ex: "PANDORA")
        # B) A propagação estiver DESLIGADA (o logger é independente)
        is_root = "." not in self.logger.name
        
        if is_root or not self.logger.propagate:
            # Console Handler
            console_h = logging.StreamHandler()
            console_h.setFormatter(formatter)
            self.logger.addHandler(console_h)

            # File Handler
            if to_file:
                file_h = logging.FileHandler(to_file, encoding="utf-8")
                file_h.setFormatter(formatter)
                self.logger.addHandler(file_h)

        # 2. Memory Handler (Sempre adicionamos para permitir get_history individual)
        mem_h = InMemoryHandler()
        self.logger.addHandler(mem_h)
        
        return mem_h

    # --- Atalhos para os métodos nativos ---
    def debug(self, msg, *args, **kwargs): self.logger.debug(msg, *args, **kwargs)
    def info(self, msg, *args, **kwargs): self.logger.info(msg, *args, **kwargs)
    def warning(self, msg, *args, **kwargs): self.logger.warning(msg, *args, **kwargs)
    def error(self, msg, *args, **kwargs): self.logger.error(msg, *args, **kwargs)
    def critical(self, msg, *args, **kwargs): self.logger.critical(msg, *args, **kwargs)

    # --- Gestão dos logs em memória ---
    def get_history(self) -> List[Dict]:
        """Retorna a lista de dicionários capturados."""
        return self.memory_handler.records

    def get_history_json(self, indent: int = 2) -> str:
        """Retorna os logs prontos para salvar como arquivo .json no S3."""
        return self.memory_handler.to_json(indent=indent)

    def clear_history(self) -> None:
        """Limpa o cache de memória da execução atual."""
        self.memory_handler.clear()