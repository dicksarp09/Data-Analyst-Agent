import os
import numpy as np
from typing import Optional, Dict, Any
from app.core.tracing import trace


class ModelEngine:
    def __init__(self):
        self.min_data_points = int(os.environ.get("MIN_DATA_POINTS", 50))
        self._model = None
        self._feature_names = None

    def train_regression(self, X, y) -> Dict[str, Any]:
        trace("model_train_start", {
            "X_shape": X.shape if hasattr(X, 'shape') else len(X),
            "y_shape": y.shape if hasattr(y, 'shape') else len(y)
        })

        try:
            import sklearn
            from sklearn.linear_model import LinearRegression
            from sklearn.model_selection import train_test_split

            if len(X) < self.min_data_points:
                return {
                    "success": False,
                    "error": f"Insufficient data points: {len(X)} < {self.min_data_points}",
                    "prediction": None,
                    "confidence": 0.0,
                    "method": "regression"
                }

            X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
            y_clean = y.fillna(0)

            if len(X_clean) < 10:
                return {
                    "success": False,
                    "error": "Insufficient clean data for training",
                    "prediction": None,
                    "confidence": 0.0,
                    "method": "regression"
                }

            X_train, X_test, y_train, y_test = train_test_split(
                X_clean, y_clean, test_size=0.2, random_state=42
            )

            model = LinearRegression()
            model.fit(X_train, y_train)

            train_score = model.score(X_train, y_train)
            test_score = model.score(X_test, y_test)

            confidence = min(test_score, 1.0) if test_score > 0 else 0.0

            self._model = model
            self._feature_names = list(X_clean.columns)

            trace("model_train_complete", {
                "train_score": train_score,
                "test_score": test_score,
                "confidence": confidence
            })

            return {
                "success": True,
                "train_r2": train_score,
                "test_r2": test_score,
                "confidence": confidence,
                "method": "linear_regression",
                "feature_count": len(self._feature_names)
            }

        except ImportError:
            trace("model_train_sklearn_missing", {})
            return {
                "success": False,
                "error": "scikit-learn not installed",
                "prediction": None,
                "confidence": 0.0,
                "method": "regression"
            }
        except Exception as e:
            trace("model_train_error", {"error": str(e)})
            return {
                "success": False,
                "error": str(e),
                "prediction": None,
                "confidence": 0.0,
                "method": "regression"
            }

    def predict(self, X) -> Optional[float]:
        if self._model is None:
            return None

        try:
            X_clean = X.fillna(0).replace([np.inf, -np.inf], 0)
            prediction = self._model.predict(X_clean)
            return float(prediction[0]) if len(prediction) > 0 else None
        except Exception as e:
            trace("model_predict_error", {"error": str(e)})
            return None

    def get_feature_importance(self) -> list:
        if self._model is None or not hasattr(self._model, 'coef_'):
            return []

        importance = list(zip(self._feature_names, np.abs(self._model.coef_)))
        importance.sort(key=lambda x: x[1], reverse=True)

        return [{"feature": f, "importance": float(i)} for f, i in importance[:10]]
