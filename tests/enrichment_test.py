import pandas as pd
import json
import tempfile
from src.data_enrichment import CreditEnricher


def test_enrich_basic():
    mock_data = {
        "123": {
            "puntaje": 700,
            "morosidad": 0.1,
            "ultima_consulta": "2024-01-01"
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        json.dump(mock_data, f)
        path = f.name

    enricher = CreditEnricher(path)

    df = pd.DataFrame([{"cedula": "123"}])
    result = enricher.enrich(df)

    assert "puntaje_credito" in result.columns
    assert result.iloc[0]["puntaje_credito"] == 700

def test_enrich_missing_cedula():
    mock_data = {}

    import tempfile, json
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        json.dump(mock_data, f)
        path = f.name

    enricher = CreditEnricher(path)

    import pandas as pd
    df = pd.DataFrame([{"cedula": "999"}])
    result = enricher.enrich(df)

    assert result.iloc[0]["puntaje_credito"] == 500
    assert result.iloc[0]["morosidad"] == 0.5
    assert result.iloc[0]["ultima_consulta"] is None

import pytest

def test_enrich_missing_column():
    import tempfile, json, pandas as pd

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        json.dump({}, f)
        path = f.name

    enricher = CreditEnricher(path)

    df = pd.DataFrame([{"otra_col": 1}])

    with pytest.raises(KeyError):
        enricher.enrich(df)