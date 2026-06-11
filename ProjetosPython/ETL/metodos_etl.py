import math, datetime
import pandas as pd

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