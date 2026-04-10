def validate_quality(data):
    if data is None or len(data) == 0:
        return {"success": False, "errors": ["empty dataset"]}

    errors = []

    for i, row in enumerate(data):
        if "income" not in row or row["income"] is None:
            errors.append(f"row {i}: missing income")
        if "amount" not in row or row["amount"] is None:
            errors.append(f"row {i}: missing amount")

    if errors:
        return {"success": False, "errors": errors}

    return {"success": True, "errors": []}


def enforce_quality(data):
    result = validate_quality(data)
    if not result["success"]:
        raise ValueError(f"data quality validation failed: {result['errors']}")
    return data