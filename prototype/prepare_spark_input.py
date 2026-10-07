from pathlib import Path

from .spark_streaming import prepare_file_input


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    prepare_file_input(
        root / "artifacts/step4/local-demo/input-observations.ndjson",
        root / "artifacts/step4/spark-input",
    )

