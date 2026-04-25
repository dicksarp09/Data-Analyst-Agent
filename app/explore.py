import os
import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


def _get_env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


def _get_env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


class CorrelationEngine:
    def __init__(self):
        self.threshold = _get_env_float("CORRELATION_THRESHOLD", 0.6)

    def compute_correlations(self, df: pd.DataFrame) -> list[dict]:
        import scipy.stats as stats

        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) < 2:
            return []

        signals = []
        col_list = list(numeric_cols)

        for i in range(len(col_list)):
            for j in range(i + 1, len(col_list)):
                col_a, col_b = col_list[i], col_list[j]
                vals_a = df[col_a].dropna()
                vals_b = df[col_b].dropna()

                common_idx = vals_a.index.intersection(vals_b.index)
                if len(common_idx) < 3:
                    continue

                x = vals_a.loc[common_idx]
                y = vals_b.loc[common_idx]

                if x.std() == 0 or y.std() == 0:
                    continue

                r, p = stats.pearsonr(x, y)

                if abs(r) >= self.threshold:
                    signals.append({
                        "type": "correlation",
                        "pair": [col_a, col_b],
                        "strength": round(abs(r), 3),
                        "p_value": round(p, 4),
                        "method": "pearson"
                    })

        return signals


class AnomalyDetector:
    def __init__(self):
        self.z_threshold = _get_env_float("Z_SCORE_THRESHOLD", 3.0)
        self.iqr_multiplier = _get_env_float("IQR_MULTIPLIER", 1.5)

    def detect_anomalies(self, df: pd.DataFrame, time_col: Optional[str] = None) -> list[dict]:
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        signals = []

        for col in numeric_cols:
            values = df[col].dropna()
            if len(values) < 10:
                continue

            z_signals = self._zscore_detect(values, col)
            signals.extend(z_signals)

            iqr_signals = self._iqr_detect(values, col)
            signals.extend(iqr_signals)

        return signals

    def _zscore_detect(self, series: pd.Series, col_name: str) -> list[dict]:
        mean = series.mean()
        std = series.std()
        if std == 0:
            return []

        z_scores = np.abs((series - mean) / std)
        anomalies = z_scores[z_scores > self.z_threshold]

        if len(anomalies) > 0:
            severity = "high" if len(anomalies) / len(series) > 0.05 else "medium"
            return [{
                "type": "anomaly",
                "feature": col_name,
                "severity": severity,
                "method": "z_score",
                "anomaly_count": int(len(anomalies)),
                "total_count": int(len(series))
            }]

        return []

    def _iqr_detect(self, series: pd.Series, col_name: str) -> list[dict]:
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return []

        lower = q1 - self.iqr_multiplier * iqr
        upper = q3 + self.iqr_multiplier * iqr

        anomalies = series[(series < lower) | (series > upper)]

        if len(anomalies) > 0:
            return [{
                "type": "anomaly",
                "feature": col_name,
                "severity": "medium",
                "method": "iqr",
                "anomaly_count": int(len(anomalies)),
                "total_count": int(len(series))
            }]

        return []


class ClusteringEngine:
    def __init__(self):
        self.min_rows = _get_env_int("MIN_ROWS_FOR_CLUSTERING", 50)
        self.max_clusters = _get_env_int("MAX_CLUSTERS", 5)

    def cluster(self, df: pd.DataFrame) -> list[dict]:
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) < 2:
            return []

        features = df[numeric_cols].dropna()
        if len(features) < self.min_rows:
            return []

        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        scaled = scaler.fit_transform(features)

        n_clusters = min(self.max_clusters, len(features) // 10)
        if n_clusters < 2:
            return []

        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        labels = kmeans.fit_predict(scaled)

        cluster_sizes = pd.Series(labels).value_counts().to_dict()

        return [{
            "type": "cluster",
            "feature_group": list(numeric_cols),
            "clusters": n_clusters,
            "cluster_sizes": {str(k): v for k, v in cluster_sizes.items()},
            "inertia": round(kmeans.inertia_, 2)
        }]


class DistributionShiftDetector:
    def __init__(self):
        self.shift_threshold = _get_env_float("SHIFT_THRESHOLD", 0.3)
        self.n_quantiles = 4

    def detect_shifts(self, df: pd.DataFrame) -> list[dict]:
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        signals = []

        for col in numeric_cols:
            values = df[col].dropna()
            if len(values) < 20:
                continue

            shift_signal = self._ks_test(values, col)
            if shift_signal:
                signals.append(shift_signal)

        return signals

    def _ks_test(self, series: pd.Series, col_name: str) -> Optional[dict]:
        from scipy.stats import ks_2samp

        n = len(series)
        split_idx = n // 2

        early = series.iloc[:split_idx]
        late = series.iloc[split_idx:]

        stat, p = ks_2samp(early, late)

        shift_score = stat
        if shift_score >= self.shift_threshold:
            return {
                "type": "distribution_shift",
                "feature": col_name,
                "shift_score": round(shift_score, 3),
                "p_value": round(p, 4),
                "method": "ks_test",
                "early_mean": round(early.mean(), 2),
                "late_mean": round(late.mean(), 2)
            }

        return None


class TrendBreakDetector:
    def __init__(self):
        self.window = _get_env_int("ROLLING_WINDOW", 5)

    def detect_trends(self, df: pd.DataFrame, time_col: str) -> list[dict]:
        if time_col not in df.columns:
            return []

        signals = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        df_sorted = df.sort_values(time_col)

        for col in numeric_cols:
            values = df_sorted[col].dropna()
            if len(values) < self.window * 2:
                continue

            trend_signal = self._rolling_change(values, col, time_col)
            if trend_signal:
                signals.append(trend_signal)

        return signals

    def _rolling_change(self, series: pd.Series, col_name: str, time_col: str) -> Optional[dict]:
        rolling = series.rolling(window=self.window, min_periods=2).mean()
        diffs = rolling.diff().abs()

        max_change_idx = diffs.idxmax()
        if pd.isna(max_change_idx):
            return None

        max_change = diffs.max()
        if max_change > 0:
            return {
                "type": "trend_break",
                "feature": col_name,
                "timestamp": str(max_change_idx),
                "change_magnitude": round(max_change, 2),
                "method": "rolling_change"
            }

        return None


class SignalScorer:
    def __init__(self):
        pass

    def score_signals(self, signals: list[dict]) -> list[dict]:
        scored = []
        for sig in signals:
            sig_copy = sig.copy()

            if sig["type"] == "correlation":
                sig_copy["confidence"] = min(1.0, 1.0 - sig.get("p_value", 0))
                sig_copy["strength"] = sig.get("strength", 0)
            elif sig["type"] == "anomaly":
                anomaly_ratio = sig.get("anomaly_count", 0) / max(1, sig.get("total_count", 1))
                sig_copy["confidence"] = 1.0 - anomaly_ratio
                sig_copy["strength"] = min(1.0, anomaly_ratio * 2)
            elif sig["type"] == "cluster":
                sig_copy["confidence"] = 0.8
                sig_copy["strength"] = sig.get("clusters", 0) / 10
            elif sig["type"] == "distribution_shift":
                sig_copy["confidence"] = min(1.0, 1.0 - sig.get("p_value", 0))
                sig_copy["strength"] = sig.get("shift_score", 0)
            elif sig["type"] == "trend_break":
                sig_copy["confidence"] = 0.7
                sig_copy["strength"] = min(1.0, sig.get("change_magnitude", 0) / 100)
            else:
                sig_copy["confidence"] = 0.5
                sig_copy["strength"] = 0.5

            scored.append(sig_copy)

        return scored


class SignalStore:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)

    def save_signals(self, session_id: str, signals: list[dict]) -> None:
        session_dir = self.data_dir / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        signal_path = session_dir / "signals.json"
        with open(signal_path, "w") as f:
            json.dump({"signals": signals}, f, indent=2, default=str)

    def load_signals(self, session_id: str) -> Optional[list[dict]]:
        signal_path = self.data_dir / session_id / "signals.json"
        if not signal_path.exists():
            return None

        with open(signal_path) as f:
            data = json.load(f)
            return data.get("signals", [])


class SignalOrchestrator:
    def __init__(self, data_dir: str):
        self.correlation = CorrelationEngine()
        self.anomaly = AnomalyDetector()
        self.clustering = ClusteringEngine()
        self.distribution_shift = DistributionShiftDetector()
        self.trend_break = TrendBreakDetector()
        self.scorer = SignalScorer()
        self.store = SignalStore(data_dir)

    def run_exploration(self, session_id: str, df: pd.DataFrame) -> list[dict]:
        all_signals = []

        correlation_signals = self.correlation.compute_correlations(df)
        all_signals.extend(correlation_signals)

        time_col = None
        for col in df.columns:
            if "date" in col.lower() or "time" in col.lower():
                time_col = col
                break

        if time_col:
            trend_signals = self.trend_break.detect_trends(df, time_col)
            all_signals.extend(trend_signals)

        anomaly_signals = self.anomaly.detect_anomalies(df, time_col)
        all_signals.extend(anomaly_signals)

        # Sample large datasets to prevent memory issues (max 10k rows for clustering)
        df_sample = df
        if len(df) > 10000:
            df_sample = df.sample(n=10000, random_state=42)
        
        cluster_signals = self.clustering.cluster(df_sample)
        all_signals.extend(cluster_signals)

        shift_signals = self.distribution_shift.detect_shifts(df_sample)
        all_signals.extend(shift_signals)

        max_signals = _get_env_int("MAX_SIGNALS", 50)
        all_signals = all_signals[:max_signals]

        scored_signals = self.scorer.score_signals(all_signals)

        self.store.save_signals(session_id, scored_signals)

        return scored_signals