import os
import json
import shutil
import tempfile
from app.phase4 import Phase4Orchestrator, PlotRenderer


def test_plot_renderer_and_phase4_run():
    tmp = tempfile.mkdtemp()
    data_dir = tmp
    session_id = "test-session"
    session_dir = os.path.join(data_dir, session_id)
    os.makedirs(session_dir, exist_ok=True)

    # Create a simple clean.csv
    csv_path = os.path.join(session_dir, "clean.csv")
    with open(csv_path, "w") as f:
        f.write("date,category,value\n")
        f.write("2020-01-01,A,10\n")
        f.write("2020-01-02,A,15\n")
        f.write("2020-01-03,B,5\n")
        f.write("2020-01-04,B,8\n")

    # Minimal hypothesis graph with one node
    hypothesis_graph = {
        "hypothesis_graph": {
            "nodes": [
                {"id": "h1", "description": "Category A has higher values", "type": "segment-based", "features": ["category", "value"], "signal_support": 0.6}
            ]
        }
    }

    # Execution result with accepted hypothesis
    execution_result = {
        "accepted_hypotheses": ["h1"],
        "rejected_hypotheses": [],
        "execution_history": [
            {"hypothesis_id": "h1", "evidence": {"confidence": 0.7, "effect_size": 0.3}, "decision": "accept"}
        ],
        "insights": []
    }

    orchestrator = Phase4Orchestrator(data_dir)
    result = orchestrator.run(session_id, execution_result, hypothesis_graph, schema={"columns": ["date","category","value"], "inferred_types": {"date":"str","category":"str","value":"float"}})

    assert isinstance(result, dict)
    assert "insights" in result
    assert "plots" in result

    # PlotRenderer test: create a simple plot spec
    renderer = PlotRenderer(data_dir)
    plot_spec = {"x": "category", "y": "value", "type": "bar", "question": "How do groups compare?"}
    plot = renderer.render(plot_spec, "h1", session_id, {"columns": ["date","category","value"], "inferred_types": {"date":"str","category":"str","value":"float"}})
    assert plot is not None
    assert plot.get("format") == "base64_png"

    shutil.rmtree(tmp)
