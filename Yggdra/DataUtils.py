from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Union, Any, Optional

class DataUtils:
    """
    Classe utilitária para orquestração de tempo e partições.
    Suporta defasagem (D-x), reprocessamento condicional e múltiplos formatos.
    """

    @staticmethod
    def format_partition(date_ref: Any, p_type: str) -> str:
        """Fábrica de formatos de data (Suporta YYYYMMDD, YYYY-MM-DD, datetime)."""
        if isinstance(date_ref, (int, str)):
            str_date = str(date_ref).strip()
            try:
                if "-" in str_date:
                    dt = datetime.strptime(str_date, "%Y-%m-%d")
                elif len(str_date) == 6:
                    dt = datetime.strptime(str_date, "%Y%m")
                else:
                    dt = datetime.strptime(str_date[:8], "%Y%m%d")
            except ValueError:
                return str(date_ref)
        else:
            dt = date_ref

        formats = {
            "ano": "%Y",
            "mes": "%m",
            "anomes": "%Y%m",
            "anomesdia": "%Y%m%d",
            "data": "%Y-%m-%d"
        }
        return dt.strftime(formats.get(p_type.lower(), "%Y-%m-%d"))

    @staticmethod
    def _get_base_dates(
        p_type: str,
        dt_ini: Union[int, str], 
        dt_fim: Union[int, str], 
        reprocessamento: bool, 
        range_reprocessamento: int, 
        dia_corte: Optional[int] = None,
        defasagem: int = 0
    ) -> List[datetime]:
        """
        Calcula a lista base de datas considerando defasagem e reprocessamento.
        """
        hoje = datetime.combine(date.today(), datetime.min.time())
        hoje_dia = date.today().day
        is_daily = p_type.lower() in ["data", "anomesdia"]

        # 1. Definição do Salto (Delta)
        step = timedelta(days=1) if is_daily else relativedelta(months=1)

        # 2. Cálculo Automático (Âncora e Defasagem)
        if int(dt_ini) == 190001 and int(dt_fim) == 190001:
            # Define o ponto de referência (hoje ou 1º do mês)
            anchor = hoje if is_daily else hoje.replace(day=1)
            
            # APLICA A DEFASAGEM: Recua o ponto final antes de qualquer outra lógica
            dt_fim_dt = anchor - (step * defasagem)
            
            # Padrão: Apenas o dia/mês defasado
            dt_ini_dt = dt_fim_dt 

            # 3. Lógica de Reprocessamento Condicional
            if reprocessamento:
                # Se dia_corte for None ou coincidir com hoje, autoriza o range
                deve_reprocessar = (dia_corte is None) or (hoje_dia == dia_corte)
                
                if deve_reprocessar:
                    # Recua o range a partir do ponto já defasado
                    dt_ini_dt = dt_fim_dt - (step * range_reprocessamento)
        
        else:
            # Caso de Datas Manuais (sempre obedece o range informado)
            str_ini, str_fim = str(dt_ini), str(dt_fim)
            fmt_ini = "%Y%m%d" if len(str_ini) > 6 else "%Y%m"
            fmt_fim = "%Y%m%d" if len(str_fim) > 6 else "%Y%m"
            dt_ini_dt = datetime.strptime(str_ini, fmt_ini)
            dt_fim_dt = datetime.strptime(str_fim, fmt_fim)

        if dt_fim_dt < dt_ini_dt:
            return []

        # 4. Geração da Lista
        dates = []
        current = dt_ini_dt
        while current <= dt_fim_dt:
            dates.append(current)
            current += step
            
        return dates

    @staticmethod
    def generate_partitions(
        p_type: str,
        dt_ini: Union[int, str] = 190001,
        dt_fim: Union[int, str] = 190001,
        reprocessamento: bool = False,
        range_reprocessamento: int = 0,
        dia_corte: Optional[int] = None,
        defasagem: int = 0
    ) -> List[str]:
        """Gera a lista final de strings formatadas para o loop do ETL."""
        base_dates = DataUtils._get_base_dates(
            p_type, dt_ini, dt_fim, reprocessamento, range_reprocessamento, dia_corte, defasagem
        )
        return [DataUtils.format_partition(d, p_type) for d in base_dates]