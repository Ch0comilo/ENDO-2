from src.data_validation import validate_quality

def test_validate_empty():
    result = validate_quality([])

    assert result["success"] is False
    assert "empty dataset" in result["errors"]