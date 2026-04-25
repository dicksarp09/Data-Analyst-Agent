import os
import re
from pathlib import Path
from typing import TypedDict
import json

from dotenv import load_dotenv
load_dotenv()


class QuestionType:
    DESCRIPTIVE = "descriptive"
    TREND = "trend" 
    COMPARATIVE = "comparative"
    CAUSAL = "causal"
    ANOMALY = "anomaly"
    CORRELATION = "correlation"
    SEGMENT_DISCOVERY = "segment_discovery"
    CHANGE_DETECTION = "change_detection"


QUESTION_PATTERNS = {
    QuestionType.DESCRIPTIVE: [
        r"^what (is|are|does) the (total|average|mean|sum|count|number)",
        r"^how many",
        r"^show (me )?(the )?(total|average|sum|count)",
        r"^give me( the)? statistics?",
    ],
    QuestionType.TREND: [
        r"why( did)? .* (drop|increase|decrease|change|rise|fall|grow)",
        r"how did .* (change|evolve|develop|grow|progress)",
        r"what.* trend",
        r"over time",
        r"(recent|lately|past|from .* to)",
    ],
    QuestionType.COMPARATIVE: [
        r"(mobile|desktop|ios|android|web)",
        r"(better|worse|more|less|higher|lower).*than",
        r"compare",
        r"(by|per|for each) .*(category|country|segment|device|region)",
        r"which .*(perform|better|best|worst)",
    ],
    QuestionType.CAUSAL: [
        r"why( is|did|does|was)",
        r"what caused",
        r"(because|due to|reason for)",
        r"what made .* (drop|change|increase)",
    ],
    QuestionType.ANOMALY: [
        r"(unusual|anomal|odd|strange|exceptional)",
        r"what.* (outlier|deviation)",
        r"anything.* (weird|odd|unusual)",
        r"detect",
    ],
    QuestionType.CORRELATION: [
        r"(related|correlated|connected)",
        r".*affect.*",
        r"(influence|impact)",
        r"does .* (relate|connect|impact)",
    ],
    QuestionType.SEGMENT_DISCOVERY: [
        r"what (user|customer|group|segment|cluster)",
        r"different (type|kind|group|segment)",
        r"behavior.* (pattern|group)",
        r"discover",
    ],
    QuestionType.CHANGE_DETECTION: [
        r"(before|after) .* (change|event|release|update)",
        r"what changed",
        r"shift.* (before|after)",
        r"(increase|drop).* (suddenly|abrupt)",
    ],
}


def detect_question_type(query: str) -> str:
    query_lower = query.lower().strip()
    
    for qtype, patterns in QUESTION_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, query_lower):
                return qtype
    
    return QuestionType.DESCRIPTIVE


def build_execution_plan(question_type: str, query: str) -> dict:
    """Build the phases that need to run based on question type."""
    
    plan = {
        "phases": [],
        "plots_needed": [],
        "narrative_style": "standard",
    }
    
    if question_type == QuestionType.DESCRIPTIVE:
        plan["phases"] = [0]
        plan["plots_needed"] = ["bar", "histogram"]
        plan["narrative_style"] = "direct"
        
    elif question_type == QuestionType.TREND:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["line", "time_series"]
        plan["narrative_style"] = "temporal"
        
    elif question_type == QuestionType.COMPARATIVE:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["bar", "grouped_bar", "boxplot"]
        plan["narrative_style"] = "comparative"
        
    elif question_type == QuestionType.CAUSAL:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["scatter", "line", "boxplot"]
        plan["narrative_style"] = "causal"
        
    elif question_type == QuestionType.ANOMALY:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["boxplot", "scatter"]
        plan["narrative_style"] = "anomaly"
        
    elif question_type == QuestionType.CORRELATION:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["scatter", "heatmap"]
        plan["narrative_style"] = "correlation"
        
    elif question_type == QuestionType.SEGMENT_DISCOVERY:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["cluster", "bar", "scatter"]
        plan["narrative_style"] = "discovery"
        
    elif question_type == QuestionType.CHANGE_DETECTION:
        plan["phases"] = [0, 1, 2, 3]
        plan["plots_needed"] = ["line", "histogram", "before_after"]
        plan["narrative_style"] = "temporal"
    
    return plan


def generate_type_aware_prompt(question: str, question_type: str, insights: list, plots: list, execution_result: dict) -> str:
    """Generate contextual prompts for GROQ based on question type."""
    
    insight_text = ""
    if insights:
        insight_text = "\n".join([
            f"- {i.get('insight', '')} (confidence: {i.get('confidence', 0):.2f})"
            for i in insights[:3]
        ])
    
    base_prompt = f"""You are a data analyst AI. Answer the user's question based on the analysis that has been performed.

User Question: {question}
Question Type: {question_type}

Analysis Insights:
{insight_text if insight_text else "No insights available yet."}

"""
    if question_type == QuestionType.DESCRIPTIVE:
        base_prompt += """Provide a direct, factual answer with the key metrics. Include any relevant numbers."""
        
    elif question_type == QuestionType.TREND:
        base_prompt += """Explain the trend over time. Mention specific time periods, changes observed, and any patterns identified.
Include temporal context - when did changes occur?"""
        
    elif question_type == QuestionType.COMPARATIVE:
        base_prompt += """Compare the segments/groups.Highlight differences, identify which performs better/worse.
Include specific comparisons between groups."""
        
    elif question_type == QuestionType.CAUSAL:
        base_prompt += """Address the causal question carefully. Present the most likely explanations based on evidence.
If multiple causes are possible, present them with their supporting evidence.
Note uncertainty - correlation does not prove causation."""
        
    elif question_type == QuestionType.ANOMALY:
        base_prompt += """Describe the anomalies detected. When did they occur? What's unusual?
Provide context about whether they represent real issues or data quality."""
        
    elif question_type == QuestionType.CORRELATION:
        base_prompt += """Explain the relationship found. Be clear about correlation vs causation.
Note that correlation shows association but not cause."""
        
    elif question_type == QuestionType.SEGMENT_DISCOVERY:
        base_prompt += """Describe the user segments/groups discovered. What characterizes each segment?
How do the segments differ in behavior?"""
        
    elif question_type == QuestionType.CHANGE_DETECTION:
        base_prompt += """Describe what changed. When did the change occur? Before vs after comparison.
What might have caused the change?"""
    
    base_prompt += """

Provide a clear, well-structured answer. If you need to show data, include it in a table format.
Keep it concise but informative."""
    
    return base_prompt


class IntelligentChatRouter:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.groq_model = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
        api_key = os.environ.get("GROQ_API_KEY")
        self.groq_client = None
        if api_key:
            try:
                from groq import Groq
                self.groq_client = Groq(api_key=api_key)
            except Exception:
                pass
    
    def route_and_execute(self, session_id: str, query: str, phase_executors: dict) -> dict:
        """Main entry point - parse question, execute needed phases, generate response."""
        
        question_type = detect_question_type(query)
        
        execution_plan = build_execution_plan(question_type, query)
        
        executed_phases = set()
        
        for phase in execution_plan["phases"]:
            if phase == 0:
                if not self._has_phase_data(session_id, "clean.csv"):
                    phase_executors["upload"](session_id)
                executed_phases.add(0)
            elif phase == 1:
                if not self._has_phase_data(session_id, "signals.json"):
                    phase_executors["explore"](session_id)
                executed_phases.add(1)
            elif phase == 2:
                if not self._has_phase_data(session_id, "hypothesis_graph.json"):
                    phase_executors["hypotheses"](session_id)
                executed_phases.add(2)
            elif phase == 3:
                if not self._has_phase_data(session_id, "execution_results.json"):
                    phase_executors["execute"](session_id)
                executed_phases.add(3)
        
        insights = self._load_insights(session_id)
        plots = self._load_plots(session_id)
        execution_result = self._load_execution(session_id)
        
        prompt = generate_type_aware_prompt(
            query, question_type, insights, plots, execution_result
        )
        
        answer = self._call_groq(prompt)
        
        return {
            "question_type": question_type,
            "executed_phases": list(executed_phases),
            "phases_needed": execution_plan["phases"],
            "plots_needed": execution_plan["plots_needed"],
            "insights": insights,
            "plots": plots,
            "answer": answer,
        }
    
    def _has_phase_data(self, session_id: str, filename: str) -> bool:
        path = self.data_dir / session_id / filename
        return path.exists()
    
    def _load_insights(self, session_id: str) -> list:
        path = self.data_dir / session_id / "phase4_insights.json"
        if not path.exists():
            return []
        with open(path) as f:
            data = json.load(f)
            return data.get("insights", [])
    
    def _load_plots(self, session_id: str) -> list:
        path = self.data_dir / session_id / "phase4_insights.json"
        if not path.exists():
            return []
        with open(path) as f:
            data = json.load(f)
            return data.get("plots", [])
    
    def _load_execution(self, session_id: str) -> dict:
        path = self.data_dir / session_id / "execution_results.json"
        if not path.exists():
            return {}
        with open(path) as f:
            return json.load(f)
    
    def _call_groq(self, prompt: str) -> str:
        if not self.groq_client:
            return "GROQ not configured. Please set GROQ_API_KEY in .env"
        
        try:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500,
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error generating response: {str(e)}"