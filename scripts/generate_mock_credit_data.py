import json
import os
import random
from datetime import datetime, timedelta


def generate_mock_data(n=10):
    data = {}
    for i in range(n):
        cedula = str(1000000000 + i)
        data[cedula] = {
            "puntaje": random.randint(300, 850),
            "morosidad": round(random.uniform(0, 1), 2),
            "ultima_consulta": (
                datetime.utcnow() - timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d")
        }
    return data


def main():
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(base_path, "data", "reference", "credito_mock.json")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    data = generate_mock_data(10)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()