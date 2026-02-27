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
                "module": record.module,
                "line": record.lineno
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
        name: str = "app",
        level: str = "INFO",
        to_file: Optional[str] = None,
        propagate: bool = False,
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self._LEVELS.get(level.upper(), logging.INFO))
        self.logger.propagate = propagate

        # Inicializa ou recupera o handler de memória
        self.memory_handler = self._setup_handlers(to_file)

    def _setup_handlers(self, to_file: Optional[str]) -> InMemoryHandler:
        """Configura os handlers apenas se eles ainda não existirem no logger."""
        
        # Se já tiver handlers, busca o InMemoryHandler existente
        for handler in self.logger.handlers:
            if isinstance(handler, InMemoryHandler):
                return handler

        # Formatter padrão para saídas legíveis (Console/Arquivo)
        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        date_fmt = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

        # 1. Console Handler
        console_h = logging.StreamHandler()
        console_h.setFormatter(formatter)
        self.logger.addHandler(console_h)

        # 2. File Handler (Opcional)
        if to_file:
            file_h = logging.FileHandler(to_file, encoding="utf-8")
            file_h.setFormatter(formatter)
            self.logger.addHandler(file_h)

        # 3. Memory Handler (Sempre ativo para exportação JSON)
        mem_h = InMemoryHandler()
        self.logger.addHandler(mem_h)
        
        return mem_h

    # --- Atalhos para os métodos nativos ---
    # Usar self.logger.info permite capturar args e kwargs extras do logging
    def debug(self, msg, *args, **kwargs): self.logger.debug(msg, *args, **kwargs)
    def info(self, msg, *args, **kwargs): self.logger.info(msg, *args, **kwargs)
    def warning(self, msg, *args, **kwargs): self.logger.warning(msg, *args, **kwargs)
    def error(self, msg, *args, **kwargs): self.logger.error(msg, *args, **kwargs)
    def critical(self, msg, *args, **kwargs): self.logger.critical(msg, *args, **kwargs)

    # --- Gestão dos logs em memória ---
    def get_history(self) -> List[Dict]:
        """Retorna a lista de logs capturados nesta execução."""
        return self.memory_handler.records

    def get_history_json(self, indent: int = 2) -> str:
        """Retorna os logs formatados em JSON para persistência externa."""
        return self.memory_handler.to_json(indent=indent)

    def clear_history(self) -> None:
        """Limpa o cache de memória."""
        self.memory_handler.clear()