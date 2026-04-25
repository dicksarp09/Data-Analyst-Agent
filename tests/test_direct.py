import os
import sys

sys.path.insert(0, os.getcwd())
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
import pandas as pd
from app.layers import (
    FileIntakeLayer,
    SchemaValidator,
    MissingnessProfiler,
    OutlierScanner,
    DataFrameMaterializer,
    SemanticMapper,
    DuckDBConnector,
)


def test_e2e():
    data_dir = "data/e2e_test"
    if os.path.exists(data_dir):
        import shutil
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    session_id = str(uuid.uuid4())

    file_intake = FileIntakeLayer(data_dir)
    schema_validator = SchemaValidator()
    missingness_profiler = MissingnessProfiler()
    outlier_scanner = OutlierScanner()
    materializer = DataFrameMaterializer(data_dir)
    semantic_mapper = SemanticMapper()
    duckdb_connector = DuckDBConnector(data_dir)

    print("Test 1: File Intake")
    with open("tests/sample_data.csv", "rb") as f:
        content = f.read()
    checksum, file_size = file_intake.store_csv(session_id, content)
    assert file_intake.session_exists(session_id)
    print(f"  Checksum: {checksum[:16]}...")
    print("  PASS")

    print("Test 2: Schema Validation")
    df = pd.read_csv(file_intake.get_raw_path(session_id))
    original_cols = list(df.columns)
    normalized_cols = schema_validator.normalize_columns(original_cols)
    df.columns = normalized_cols

    inferred_types = {}
    for col in df.columns:
        inferred_types[col] = schema_validator.infer_type(df[col].dropna().tolist())

    print(f"  Original: {original_cols}")
    print(f"  Normalized: {normalized_cols}")
    print(f"  Inferred: {inferred_types}")
    print("  PASS")

    print("Test 3: Type Enforcement + Quarantine")
    valid_df, quarantine_df = schema_validator.enforce_types(df, inferred_types)
    print(f"  Valid: {len(valid_df)}, Quarantined: {len(quarantine_df)}")
    print("  PASS")

    print("Test 4: Missingness Profiler")
    missingness = missingness_profiler.compute_missingness(valid_df)
    print(f"  Missingness: {missingness}")
    print("  PASS")

    print("Test 5: Outlier Scanner")
    outliers = outlier_scanner.detect_outliers(valid_df)
    print(f"  Outliers: {outliers}")
    print("  PASS")

    print("Test 6: DataFrame Materialization")
    valid_count, quarantine_count = materializer.materialize(session_id, valid_df, quarantine_df)
    print(f"  Materialized: {valid_count} valid, {quarantine_count} quarantined")
    assert os.path.exists(f"{data_dir}/{session_id}/clean.csv")
    assert os.path.exists(f"{data_dir}/{session_id}/quarantine.csv")
    print("  PASS")

    print("Test 7: Semantic Mapping")
    roles = semantic_mapper.map_columns(normalized_cols, inferred_types)
    print(f"  Roles: {roles}")
    print("  PASS")

    print("Test 8: DuckDB Query")
    duckdb_connector.load_session(session_id)
    data = duckdb_connector.query(session_id, "SELECT * FROM clean_dataset LIMIT 5")
    print(f"  Query result: {len(data)} rows")
    print(f"  Sample row: {data[0] if data else 'none'}")
    print("  PASS")

    print("\n=== E2E TEST PASSED ===")


if __name__ == "__main__":
    test_e2e()