import os
import pandas as pd
from pathlib import Path
from typing import Optional
from app.core.tracing import trace


class FeatureBuilder:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.min_data_points = int(os.environ.get("MIN_DATA_POINTS", 50))

    def load_clean_data(self, session_id: str) -> Optional[pd.DataFrame]:
        clean_path = self.data_dir / session_id / "clean.csv"
        if not clean_path.exists():
            trace("feature_builder_no_data", {"session": session_id})
            return None

        try:
            df = pd.read_csv(clean_path)
            if len(df) < self.min_data_points:
                trace("feature_builder_insufficient_data", {
                    "session": session_id,
                    "rows": len(df)
                })
                return None
            return df
        except Exception as e:
            trace("feature_builder_load_error", {"error": str(e)})
            return None

    def create_features(self, df: pd.DataFrame, target_column: str) -> pd.DataFrame:
        trace("feature_builder_start", {"columns": list(df.columns)})

        features = df.copy()
        numeric_cols = features.select_dtypes(include=['int64', 'float64']).columns.tolist()

        if target_column in numeric_cols:
            numeric_cols.remove(target_column)

        for col in numeric_cols:
            if len(features) > 7:
                features[f"{col}_lag_1"] = features[col].shift(1)
                features[f"{col}_lag_2"] = features[col].shift(2)
                features[f"{col}_rolling_mean_3"] = features[col].rolling(window=3, min_periods=1).mean()
                features[f"{col}_rolling_std_3"] = features[col].rolling(window=3, min_periods=1).std()

        date_cols = [c for c in features.columns if 'date' in c.lower() or 'time' in c.lower()]
        if date_cols:
            date_col = date_cols[0]
            try:
                features['day_of_week'] = pd.to_datetime(features[date_col]).dt.dayofweek
                features['month'] = pd.to_datetime(features[date_col]).dt.month
                features['is_weekend'] = features['day_of_week'].isin([5, 6]).astype(int)
            except Exception:
                pass

        features = features.dropna()

        trace("feature_builder_complete", {
            "rows": len(features),
            "columns": len(features.columns)
        })

        return features

    def get_numeric_columns(self, df: pd.DataFrame) -> list:
        return df.select_dtypes(include=['int64', 'float64']).columns.tolist()

    def prepare_training_data(self, df: pd.DataFrame, target_column: str) -> tuple:
        features = self.create_features(df, target_column)

        if target_column not in features.columns:
            return None, None

        feature_cols = [c for c in features.columns if c != target_column]
        X = features[feature_cols]
        y = features[target_column]

        return X, y
