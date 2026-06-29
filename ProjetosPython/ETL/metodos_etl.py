import math, json
from datetime import datetime, date, timezone
import pandas as pd
import numpy as np
from decimal import Decimal
import re

class Etl:
    @staticmethod
    def normalizar_json(valor):
        # 1. nulos 
        if pd.isna(valor):
            return None
        if isinstance(valor, float) and math.isnan(valor):
            return None

        if pd.isna(valor):
            return None

        # 2. datas 
        if isinstance(valor, pd.Timestamp):
            return valor.isoformat()

        if isinstance(valor, (datetime.datetime, datetime.date)):
            return valor.isoformat()

        # 3. floats que são inteiros (ex: 6.0 → 6)
        if isinstance(valor, float):
            if valor.is_integer():
                return int(valor)
            return float(valor)

        # 4. strings
        if isinstance(valor, str):
            valor = valor.replace("\x00", "")
            valor = valor.replace("\u0000", "")
            return valor.strip()
        
        return valor
    
    @staticmethod
    def normalizar_rows(rows):
        rows_tratadas = []

        for row in rows:
            row_tratada = {}
            for chave, valor in row.items():
                row_tratada[chave] = Etl.normalizar_json(valor)
            rows_tratadas.append(row_tratada)

        return rows_tratadas
    
    @staticmethod
    def limpar_nan_para_json(obj):
        if obj is None:
            return None

        if isinstance(obj, float) and np.isnan(obj):
            return None

        if isinstance(obj, (np.floating,)):
            if np.isnan(obj):
                return None
            return float(obj)

        if isinstance(obj, (np.integer,)):
            return int(obj)

        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()

        if isinstance(obj, Decimal):
            return float(obj)

        if isinstance(obj, str):
            return obj.replace("\x00", "")

        if isinstance(obj, dict):
            return {k: Etl.limpar_nan_para_json(v) for k, v in obj.items()}

        if isinstance(obj, list):
            return [Etl.limpar_nan_para_json(v) for v in obj]

        try:
            if pd.isna(obj):
                return None
        except Exception:
            pass

        return obj
    
    @staticmethod
    def unix_to_datetime(value):
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
            try:
                value = float(value)
            except ValueError:
                return value

        if not isinstance(value, (int, float)):
            return None

        if value > 1e12:
            value = value / 1000

        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    
    @staticmethod
    def tratar_telefone(value):
        if not value:
            return None

        value = re.sub(r"\D", "", str(value))

        # DDD + telefone fixo
        if len(value) == 10:
            value = "55" + value

        # DDD + celular
        elif len(value) == 11:
            value = "55" + value

        # 55 + DDD + telefone fixo
        elif len(value) == 12:
            value = value[:4] + "9" + value[4:]

        return value

    @staticmethod
    def normalizar_none(valor):
        if valor in [None, "", "None", "null", "NULL"]:
            return None
        return valor
    
    @staticmethod
    def parse_json_dict(valor):
        valor = Etl.normalizar_none(valor)

        if valor is None:
            return {}

        if isinstance(valor, dict):
            return valor

        if isinstance(valor, str):
            try:
                convertido = json.loads(valor)
                if isinstance(convertido, dict):
                    return convertido
            except Exception:
                return {}

        return {}
    
    @staticmethod
    def ms_to_datetime(value):
        if not value:
            return None
        return pd.to_datetime(int(value), unit="ms", utc=True).isoformat()

    @staticmethod
    def tratar_value(value):
        if value is None:
            return None
        
        return round(float(value) / 100, 2)
    
    @staticmethod
    def tratar_value2(value):
        if value is None:
            return None
        
        return round(float(value) / 10, 2)