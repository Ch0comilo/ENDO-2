import json
import os
import logging
import pandas as pd


class CreditEnricher:
    def __init__(self, mock_credit_path: str):
        if not os.path.exists(mock_credit_path):
            raise FileNotFoundError(f"mock file not found: {mock_credit_path}")
        self.credit_data = self.load_mock(mock_credit_path)
        self.logger = logging.getLogger(__name__)

    def load_mock(self, path: str) -> dict:
        with open(path, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("mock json must be a dictionary")
        return data

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        if "cedula" not in df.columns:
            raise KeyError("missing column: cedula")

        scores = []
        morosidades = []
        consultas = []

        for _, row in df.iterrows():
            cedula = str(row["cedula"])
            record = self.credit_data.get(cedula)

            if record is None:
                self.logger.warning(f"cedula {cedula} not found in mock")
                scores.append(500)
                morosidades.append(0.5)
                consultas.append(None)
            else:
                try:
                    scores.append(record["puntaje"])
                    morosidades.append(record["morosidad"])
                    consultas.append(record.get("ultima_consulta"))
                except KeyError as e:
                    raise KeyError(f"invalid mock structure for cedula {cedula}: {e}")

        df = df.copy()
        df["puntaje_credito"] = scores
        df["morosidad"] = morosidades
        df["ultima_consulta"] = consultas

        return df