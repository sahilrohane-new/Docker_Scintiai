from typing import TypedDict, List, Optional, Literal, Dict, Any
from langgraph.graph import StateGraph, END

# ── node imports ───────────────────────────────────────────────
from agents.parse_agent      import parse_node
from agents.llm_rule_agent   import llm_rule_node
from agents.validate_agent   import validate_node
from agents.optimize_agent   import optimize_node
from agents.feedback_agent   import feedback_node

class GraphState(TypedDict, total=False):
    # ── inputs / selections ──
    sas_code: str
    source: str
    ddl_type: str
    target: str

    # ── parse / rule stage ──
    ast_blocks: List[Dict[str, Any]]
    pyspark_chunks: List[Dict[str, Any]]
    failed_chunks: List[str]
    chunk_status: List[Dict[str, Any]]

    # ── optimizer outputs ──
    before_code: str          # merged pre-optimization
    final_code: str           # optimized (python/sql)
    pyspark_code: str         # legacy key (keep)
    optimized_file: str       # path/filename written by optimizer

    # ── reporting / bookkeeping ──
    token_usage: Dict[str, Any]
    report_file: str
    report: Dict[str, Any]
    runtime: float
    validation_passed: bool
    retry_count: int
    abort: bool
    input_filename: str
    input_basename: str

    # ── misc / tracing ──
    llm_provider: str
    llm_cred: Dict[str, Any]
    logs: List[str]
    graph_trace: List[str]
    rule_csv: str

# ── routers ────────────────────────────────────────────────────
def route_after_parse(st: GraphState) -> Literal["llm_rule", "feedback"]:
    return "llm_rule" if st.get("ast_blocks") else "feedback"

def route_after_llm_rule(st: GraphState) -> Literal["validate", "feedback"]:
    return "feedback" if st.get("failed_chunks") else "validate"

def route_after_validation(st: GraphState) -> Literal["optimize", "feedback"]:
    # ✅ updated logic: if validation fails, go to feedback
    return "feedback" if not st.get("validation_passed") else "optimize"

def route_after_optimize(st: GraphState) -> Literal["end", "feedback"]:
    return "end"

def route_from_feedback(st: GraphState) -> Literal["optimize", "end"]:
    # ✅ updated logic: after feedback, go directly to optimize
    return "optimize" if not st.get("abort") else "end"

# ── build graph ────────────────────────────────────────────────
def build_graph() -> StateGraph:
    g = StateGraph(GraphState)

    g.add_node("parse",     parse_node)
    g.add_node("llm_rule",  llm_rule_node)
    g.add_node("validate",  validate_node)
    g.add_node("optimize",  optimize_node)
    g.add_node("feedback",  feedback_node)

    g.set_entry_point("parse")

    g.add_conditional_edges(
        "parse", route_after_parse,
        {"llm_rule": "llm_rule", "feedback": "feedback"}
    )
    g.add_conditional_edges(
        "llm_rule", route_after_llm_rule,
        {"validate": "validate", "feedback": "feedback"}
    )
    g.add_conditional_edges(
        "validate", route_after_validation,
        {"optimize": "optimize", "feedback": "feedback"}
    )
    g.add_conditional_edges(
        "optimize", route_after_optimize,
        {"end": END}
    )
    g.add_conditional_edges(
        "feedback", route_from_feedback,
        {"optimize": "optimize", "end": END}
    )

    g.set_finish_point("optimize")

    return g.compile()
