import hashlib
import os
from pathlib import Path
from datetime import datetime
from typing import Optional


class FileIntakeLayer:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def store_csv(self, session_id: str, content: bytes) -> tuple[str, int]:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        raw_path = session_dir / "raw.csv"
        raw_path.write_bytes(content)

        checksum = hashlib.sha256(content).hexdigest()
        file_size = len(content)

        return checksum, file_size

    def get_raw_path(self, session_id: str) -> Path:
        return self.data_dir / session_id / "raw.csv"

    def session_exists(self, session_id: str) -> bool:
        return (self.data_dir / session_id / "raw.csv").exists()


class SchemaValidator:
    def __init__(self):
        self._type_inference_rules = {
            r"^\d{4}-\d{2}-\d{2}$": "date",
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}": "datetime",
            r"^-?\d+\.\d+$": "float",
            r"^-?\d+$": "int",
            r"^\d+$": "id",
        }

    def normalize_column_name(self, col: str) -> str:
        col = col.lower()
        col = col.replace(" ", "_").replace("-", "_")
        col = "".join(c if c.isalnum() or c == "_" else "_" for c in col)
        return col

    def normalize_columns(self, columns: list[str]) -> list[str]:
        return [self.normalize_column_name(c) for c in columns]

    def infer_type(self, values: list) -> str:
        non_null = [v for v in values if v is not None and str(v).strip() != ""]
        if not non_null:
            return "string"

        sample = str(non_null[0])
        if self._matches(sample, r"^\d{4}-\d{2}-\d{2}$"):
            return "date"
        if self._matches(sample, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"):
            return "datetime"
        if self._matches(sample, r"^-?\d+\.\d+$"):
            return "float"
        if self._matches(sample, r"^-?\d+$"):
            return "int"
        return "string"

    def _matches(self, value: str, pattern: str) -> bool:
        import re
        return bool(re.match(pattern, value))

    def enforce_types(self, df, inferred_types: dict[str, str]) -> tuple:
        import pandas as pd

        valid_rows = []
        quarantine_rows = []
        type_map = {
            "date": "string",
            "datetime": "string",
            "int": "Int64",
            "float": "float",
            "string": "string",
        }

        for idx, row in df.iterrows():
            row_valid = True
            converted_row = {}
            for col, value in row.items():
                if pd.isna(value) or value is None:
                    converted_row[col] = None
                    continue

                target_type = inferred_types.get(col, "string")
                converted = self._convert_value(value, target_type, type_map)
                if converted is None:
                    row_valid = False
                    break
                converted_row[col] = converted

            if row_valid:
                valid_rows.append(converted_row)
            else:
                quarantine_rows.append(dict(row))

        valid_df = pd.DataFrame(valid_rows)
        quarantine_df = pd.DataFrame(quarantine_rows)

        for col, dtype in inferred_types.items():
            if dtype in type_map:
                try:
                    valid_df[col] = valid_df[col].astype(type_map[dtype])
                except (ValueError, TypeError):
                    pass

        return valid_df, quarantine_df

    def _convert_value(self, value, target_type: str, type_map: dict) -> Optional:
        import pandas as pd

        if pd.isna(value) or value is None or str(value).strip() == "":
            return None

        str_val = str(value).strip()

        try:
            if target_type == "int":
                return int(str_val)
            elif target_type == "float":
                return float(str_val)
            elif target_type in ("date", "datetime", "string"):
                return str_val
        except (ValueError, TypeError):
            return None

        return str_val


class MissingnessProfiler:
    def compute_missingness(self, df) -> dict[str, float]:
        missingness = {}
        total = len(df)

        if total == 0:
            return {col: 0.0 for col in df.columns}

        for col in df.columns:
            import pandas as pd
            missing_count = df[col].isna().sum() + (df[col] == "").sum()
            missingness[col] = missing_count / total

        return missingness

    def flag_high_missing(self, missingness: dict, threshold: float = 0.5) -> list[str]:
        return [col for col, rate in missingness.items() if rate > threshold]


class OutlierScanner:
    def __init__(self, iqr_multiplier: float = 1.5):
        self.iqr_multiplier = iqr_multiplier

    def detect_outliers(self, df) -> dict[str, str]:
        outliers = {}
        import pandas as pd
        import numpy as np

        numeric_cols = df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            values = df[col].dropna()
            if len(values) < 4:
                continue

            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1

            if iqr == 0:
                continue

            lower = q1 - self.iqr_multiplier * iqr
            upper = q3 + self.iqr_multiplier * iqr

            extreme = values[(values < lower) | (values > upper)]
            if len(extreme) > 0:
                outliers[col] = f"values beyond {self.iqr_multiplier} * IQR detected"

        return outliers


class DataFrameMaterializer:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def materialize(
        self,
        session_id: str,
        valid_df,
        quarantine_df,
    ) -> tuple[int, int]:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        valid_path = session_dir / "clean.csv"
        quarantine_path = session_dir / "quarantine.csv"

        valid_df.to_csv(valid_path, index=False)
        quarantine_df.to_csv(quarantine_path, index=False)

        return len(valid_df), len(quarantine_df)


class SemanticMapper:
    ROLE_TIME_AXIS = "time_axis"
    ROLE_METRIC = "metric"
    ROLE_ENTITY_KEY = "entity_key"
    ROLE_DIMENSION = "dimension"

    def map_columns(self, columns: list[str], inferred_types: dict[str, str]) -> dict[str, str]:
        import re

        roles = {}
        date_pattern = re.compile(r"(date|time|day|month|year|created|updated)", re.IGNORECASE)
        id_pattern = re.compile(r"(id|_id|key|uuid)", re.IGNORECASE)
        metric_pattern = re.compile(r"(amount|value|count|price|rate|total|score|qty)", re.IGNORECASE)

        for col in columns:
            col_lower = col.lower()
            inf_type = inferred_types.get(col, "string")

            if inf_type in ("date", "datetime"):
                roles[col] = self.ROLE_TIME_AXIS
            elif id_pattern.search(col_lower):
                roles[col] = self.ROLE_ENTITY_KEY
            elif metric_pattern.search(col_lower) or inf_type in ("int", "float"):
                roles[col] = self.ROLE_METRIC
            else:
                roles[col] = self.ROLE_DIMENSION

        return roles


class DuckDBConnector:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.connections: dict[str, "duckdb.DuckDBPyConnection"] = {}

    def load_session(self, session_id: str) -> None:
        import duckdb

        clean_path = self.data_dir / session_id / "clean.csv"
        if not clean_path.exists():
            raise FileNotFoundError(f"No clean data found for session {session_id}")

        conn = duckdb.connect(database=":memory:")
        conn.execute(f"CREATE TABLE clean_dataset AS SELECT * FROM read_csv_auto('{clean_path}')")
        self.connections[session_id] = conn

    def query(self, session_id: str, sql: str) -> list[dict]:
        import duckdb

        if session_id not in self.connections:
            self.load_session(session_id)

        conn = self.connections[session_id]
        result = conn.execute(sql).fetchdf()
        return result.to_dict(orient="records")

    def get_columns(self, session_id: str) -> list[str]:
        import duckdb

        if session_id not in self.connections:
            self.load_session(session_id)

        conn = self.connections[session_id]
        result = conn.execute("DESCRIBE clean_dataset").fetchall()
        return [row[0] for row in result]