"""
Integration test: runs the full pipeline (validation → enrichment → orchestrator)
with a small in-memory dataset and a temporary mock credit file.
"""

import json
import os
import tempfile
import shutil

import pandas as pd
import pytest

from src.data_validation import validate_quality, enforce_quality
from src.data_enrichment import CreditEnricher
from src.orchestrator import PipelineOrchestrator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_credit_file():
    """Write a small mock credit JSON to a temp file and clean up afterwards."""
    mock_data = {
        "111": {"puntaje": 750, "morosidad": 0.05, "ultima_consulta": "2024-06-01"},
        "222": {"puntaje": 620, "morosidad": 0.20, "ultima_consulta": "2024-03-15"},
        "333": {"puntaje": 480, "morosidad": 0.60, "ultima_consulta": "2023-12-01"},
    }
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(mock_data, f)
        path = f.name

    yield path
    os.unlink(path)


@pytest.fixture()
def tmp_base(tmp_path):
    """Isolated base directory for orchestrator logs and checkpoints."""
    return str(tmp_path)


@pytest.fixture()
def small_raw_data():
    """Three rows that represent a minimal valid dataset."""
    return [
        {"cedula": "111", "income": 5_000_000, "amount": 1_000_000},
        {"cedula": "222", "income": 3_200_000, "amount":   800_000},
        {"cedula": "333", "income": 1_500_000, "amount":   500_000},
    ]


# ---------------------------------------------------------------------------
# Helper: build the pipeline steps the same way the real entry-point would
# ---------------------------------------------------------------------------

def build_steps(raw_data, mock_credit_path):
    """
    Returns a list of (name, callable) steps that mirror the production pipeline.
    State is shared via a mutable container so each step receives the previous output.
    """
    state = {"data": None, "df": None}

    def step_validate():
        enforce_quality(raw_data)          # raises ValueError on bad data
        state["data"] = raw_data

    def step_enrich():
        enricher = CreditEnricher(mock_credit_path)
        df = pd.DataFrame(state["data"])
        state["df"] = enricher.enrich(df)

    return [
        ("validate", step_validate),
        ("enrich",   step_enrich),
    ], state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFullPipeline:

    def test_pipeline_completes_successfully(
        self, small_raw_data, mock_credit_file, tmp_base
    ):
        """Happy path: all steps finish without raising."""
        steps, state = build_steps(small_raw_data, mock_credit_file)
        orchestrator = PipelineOrchestrator(base_path=tmp_base)
        orchestrator.run(steps)   # must not raise

        df = state["df"]
        assert df is not None, "enriched DataFrame was never produced"
        assert len(df) == 3

    def test_enriched_columns_present(
        self, small_raw_data, mock_credit_file, tmp_base
    ):
        """All three credit columns must appear after enrichment."""
        steps, state = build_steps(small_raw_data, mock_credit_file)
        PipelineOrchestrator(base_path=tmp_base).run(steps)

        df = state["df"]
        for col in ("puntaje_credito", "morosidad", "ultima_consulta"):
            assert col in df.columns, f"missing column: {col}"

    def test_enriched_values_match_mock(
        self, small_raw_data, mock_credit_file, tmp_base
    ):
        """Credit scores must match the values in the mock file exactly."""
        steps, state = build_steps(small_raw_data, mock_credit_file)
        PipelineOrchestrator(base_path=tmp_base).run(steps)

        df = state["df"].set_index("cedula")
        assert df.loc["111", "puntaje_credito"] == 750
        assert df.loc["222", "puntaje_credito"] == 620
        assert df.loc["333", "puntaje_credito"] == 480

    def test_unknown_cedula_gets_default_score(
        self, mock_credit_file, tmp_base
    ):
        """A cedula absent from the mock must receive the fallback score of 500."""
        raw = [{"cedula": "999", "income": 2_000_000, "amount": 400_000}]
        steps, state = build_steps(raw, mock_credit_file)
        PipelineOrchestrator(base_path=tmp_base).run(steps)

        row = state["df"].iloc[0]
        assert row["puntaje_credito"] == 500
        assert row["morosidad"]       == 0.5
        assert row["ultima_consulta"] is None

    def test_pipeline_stops_on_invalid_data(
        self, mock_credit_file, tmp_base
    ):
        """Rows with None income must cause the pipeline to raise ValueError."""
        bad_data = [
            {"cedula": "111", "income": None,      "amount": 500_000},
            {"cedula": "222", "income": 3_000_000, "amount": None},
        ]
        steps, _ = build_steps(bad_data, mock_credit_file)
        orchestrator = PipelineOrchestrator(base_path=tmp_base)

        with pytest.raises(ValueError, match="data quality validation failed"):
            orchestrator.run(steps)

    def test_checkpoint_resumes_after_validate(
        self, small_raw_data, mock_credit_file, tmp_base
    ):
        """
        If a checkpoint already marks 'validate' as done, the orchestrator
        must skip it and proceed directly to 'enrich'.
        """
        import json, os
        checkpoint_dir = os.path.join(tmp_base, "data", "checkpoints")
        os.makedirs(checkpoint_dir, exist_ok=True)
        with open(os.path.join(checkpoint_dir, "checkpoint.json"), "w") as f:
            json.dump({"last_step": "validate"}, f)

        steps, state = build_steps(small_raw_data, mock_credit_file)
        # Manually pre-populate state so 'enrich' has something to work with
        state["data"] = small_raw_data

        PipelineOrchestrator(base_path=tmp_base).run(steps)

        assert state["df"] is not None
        assert len(state["df"]) == 3

    def test_execution_log_written(
        self, small_raw_data, mock_credit_file, tmp_base
    ):
        """After a successful run the execution log must contain INFO entries."""
        steps, _ = build_steps(small_raw_data, mock_credit_file)
        PipelineOrchestrator(base_path=tmp_base).run(steps)

        log_path = os.path.join(tmp_base, "logs", "execution.log")
        assert os.path.exists(log_path), "execution.log was not created"

        with open(log_path) as f:
            lines = f.readlines()

        assert len(lines) >= 2, "expected at least one log entry per step"
        for line in lines:
            entry = json.loads(line)
            assert "timestamp" in entry
            assert "level"     in entry
            assert "message"   in entry