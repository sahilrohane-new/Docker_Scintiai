from __future__ import annotations
import ast, json, re
from pathlib import Path
from typing import Dict, List, Tuple

from langchain_openai       import AzureChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

# ───────────────────── targets & validators ─────────────────────
PYTHON_TARGETS = {"pyspark", "snowpark","python"}
SQL_TARGETS    = {"databricks", "snowflake", "bigquery"}

def _clean(code: str) -> str:
    code = code.strip()
    if code.startswith("```"):
        code = re.sub(r"^```[a-zA-Z]*", "", code, 1, flags=re.S).strip()
    if code.endswith("```"):
        code = code[:-3]
    return code.strip()

def _balanced(s: str, open_ch="(", close_ch=")") -> bool:
    depth = 0
    for ch in s:
        if ch == open_ch: depth += 1
        if ch == close_ch: depth -= 1
        if depth < 0: return False
    return depth == 0

def _validate_python(code: str) -> Tuple[bool, str]:
    cleaned = _clean(code)
    if not cleaned:
        return False, "Empty code block"
    try:
        ast.parse(cleaned)
        return True, "Valid Python syntax"
    except SyntaxError as e:
        return False, f"SyntaxError: {e.msg} on line {e.lineno}"

def _validate_sql(code: str) -> Tuple[bool, str]:
    cleaned = _clean(code)
    if not cleaned:
        return False, "Empty code block"
    if not _balanced(cleaned):             # quick heuristic
        return False, "Unbalanced parentheses"
    if cleaned.count("'") % 2 or cleaned.count('"') % 2:
        return False, "Unbalanced quotes"
    return True, "Looks like valid SQL (heuristic)"

def validate_chunk(code: str, target: str) -> Tuple[bool, str]:
    if target in PYTHON_TARGETS:
        return _validate_python(code)
    if target in SQL_TARGETS:
        return _validate_sql(code)
    return True, "No validation rule for target"

# ───────────────────── LLM utils ────────────────────────────────
def _load_llm(provider: str, cred: Dict):
    if provider == "azureopenai":
        return AzureChatOpenAI(
            azure_endpoint     = cred["openai_api_base"],
            openai_api_key     = cred["openai_api_key"],
            openai_api_version = cred["openai_api_version"],
            deployment_name    = cred["deployment_name"],
            model_name         = cred["model_name"],
            temperature        = 0.0,
        )
    if provider == "gemini":
        return ChatGoogleGenerativeAI(
            model          = cred["model_name"],
            google_api_key = cred["google_api_key"],
            temperature    = 0.0,
        )
    raise ValueError("Unsupported provider")

def _prompt(source: str, target: str, ddl: str) -> ChatPromptTemplate:
    return ChatPromptTemplate.from_messages([
        ("system",
         f"You are a migration expert. Improve the generated {target.upper()} "
         f"code using the original {source.upper()} {ddl.upper()} script and the "
         f"validation error. Ensure the fixed {target.upper()} code is syntactically "
         f"correct and preserves business logic, naming, and comments."),
        ("user",
         "### Validation Error ###\n{error}\n"
         f"### Original {source.upper()} Code ###\n{{src_code}}\n"
         f"### Previous {target.upper()} Output ###\n{{gen_code}}\n\n"
         f"### Fixed {target.upper()} Code ###")
    ])

# ───────────────────── main node ────────────────────────────────
def feedback_node(state: Dict) -> Dict:
    print("🩹  Feedback Agent …")

    source   = state.get("source",   "sas").lower()
    target   = state.get("target",   "pyspark").lower()
    ddl_type = state.get("ddl_type", "general").lower()

    # path produced by Validate-Agent
    invalid_path = Path("rule_outputs") / "invalid_chunks_for_feedback.json"
    manual_path  = Path("rule_outputs") / "manual_review_chunks.json"

    if not invalid_path.exists():
        print("ℹ️  No invalid chunks to fix.")
        return state

    with invalid_path.open("r", encoding="utf-8") as f:
        failed_chunks: List[Dict] = json.load(f)

    if not failed_chunks:
        return state

    llm = _load_llm(state["llm_provider"], state["llm_cred"])
    tmpl = _prompt(source, target, ddl_type)

    fixed, manual = [], []
    for ch in failed_chunks:
        prompt = tmpl.format_prompt(
            error     = ch["reason"],
            src_code  = ch.get("source_code") or ch.get("sas_code", ""),
            gen_code  = ch.get("generated_code") or ch.get("pyspark_code", "")
        ).to_messages()

        try:
            resp = llm.invoke(prompt)
            new_code = resp.content.strip()
            ok, reason = validate_chunk(new_code, target)

            if ok:
                fixed.append({"id": ch["id"], "code": new_code})
            else:
                ch.update({"fixed_code": new_code, "reason": reason})
                manual.append(ch)

        except Exception as e:
            ch.update({"fixed_code": "", "reason": f"LLM error: {e}"})
            manual.append(ch)

    # save manual review list
    if manual:
        with manual_path.open("w", encoding="utf-8") as f:
            json.dump(manual, f, indent=2)

    # replace bad chunks in state['pyspark_chunks'] (legacy key)
    all_chunks = state.get("pyspark_chunks", [])
    fixed_map  = {c["id"]: c["code"] for c in fixed}
    updated    = [
        {"id": c["id"], "code": fixed_map.get(c["id"], c["code"])}
        for c in all_chunks
    ]

    state["pyspark_chunks"] = updated
    state["logs"].append(f"Feedback agent retried {len(fixed) + len(manual)} chunks: fixed={len(fixed)}, manual_review={len(manual)}")
    state["graph_trace"] = state.get("graph_trace", []) + ["feedback"]

    return state