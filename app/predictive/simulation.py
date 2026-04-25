import numpy as np
from typing import Dict, Any, Optional
from app.core.tracing import trace


class SimulationEngine:
    def __init__(self, model_engine):
        self.model_engine = model_engine

    def simulate_change(
        self,
        base_data,
        variable: str,
        change_pct: float
    ) -> Dict[str, Any]:
        trace("simulation_start", {
            "variable": variable,
            "change_pct": change_pct
        })

        if self.model_engine._model is None:
            return {
                "success": False,
                "error": "No trained model available",
                "prediction": None,
                "confidence": 0.0
            }

        try:
            if hasattr(base_data, 'copy'):
                modified = base_data.copy()
            elif isinstance(base_data, dict):
                modified = base_data.copy()
            else:
                modified = base_data

            if isinstance(modified, np.ndarray):
                modified = modified.copy()
            elif hasattr(modified, 'values'):
                modified = modified.values.copy()

            variable_idx = None
            if self.model_engine._feature_names and variable in self.model_engine._feature_names:
                variable_idx = self.model_engine._feature_names.index(variable)
                if isinstance(modified, np.ndarray):
                    modified[variable_idx] *= (1 + change_pct / 100)
                elif isinstance(modified, dict):
                    if variable in modified:
                        modified[variable] *= (1 + change_pct / 100)
            else:
                return {
                    "success": False,
                    "error": f"Variable '{variable}' not found in model features",
                    "prediction": None,
                    "confidence": 0.0
                }

            prediction = self.model_engine.predict(modified)

            if prediction is None:
                return {
                    "success": False,
                    "error": "Prediction failed",
                    "prediction": None,
                    "confidence": 0.0
                }

            return {
                "success": True,
                "original_variable": variable,
                "change_percent": change_pct,
                "predicted_value": prediction,
                "confidence": self.model_engine._model.score(
                    self.model_engine._model.predict(
                        np.zeros((1, len(self.model_engine._feature_names)))
                    ),
                    [0]
                ) if hasattr(self.model_engine._model, 'score') else 0.5,
                "method": "regression_simulation"
            }

        except Exception as e:
            trace("simulation_error", {"error": str(e)})
            return {
                "success": False,
                "error": str(e),
                "prediction": None,
                "confidence": 0.0
            }

    def run_scenario(
        self,
        base_data,
        changes: Dict[str, float]
    ) -> Dict[str, Any]:
        trace("scenario_start", {"changes": changes})

        modified = base_data.copy()

        for variable, change_pct in changes.items():
            if hasattr(modified, 'loc') and variable in modified.columns:
                modified[variable] *= (1 + change_pct / 100)
            elif isinstance(modified, dict) and variable in modified:
                modified[variable] *= (1 + change_pct / 100)

        prediction = self.model_engine.predict(modified)

        if prediction is None:
            return {
                "success": False,
                "error": "Scenario prediction failed",
                "prediction": None
            }

        return {
            "success": True,
            "changes": changes,
            "predicted_value": prediction,
            "method": "scenario_analysis"
        }
