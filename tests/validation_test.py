import pytest
from src.data_validation import validate_quality, enforce_quality


def test_validate_quality_success():
    data = [{"income": 1000, "amount": 200}]
    result = validate_quality(data)
    assert result["success"] is True
    assert result["errors"] == []


def test_validate_quality_failure():
    data = [{"income": None, "amount": 200}]
    result = validate_quality(data)
    assert result["success"] is False
    assert "missing income" in result["errors"][0]


def test_enforce_quality_raises():
    data = [{"income": None, "amount": 200}]
    with pytest.raises(ValueError):
        enforce_quality(data)