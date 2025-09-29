from __future__ import annotations
import ast, json, re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# ───────────────────── helpers ──────────────────────────────────
PYTHON_TARGETS = {"pyspark", "snowpark","python"}        # validate with ast
SQL_TARGETS    = {"databricks", "snowflake", "bigquery"}

def _clean(code: str) -> str:
    code = code.strip()

    # NEW: if special markers are present, keep only the OUTPUT block
    if "###OUTPUT###" in code and "###END_OUTPUT###" in code:
        try:
            code = code.split("###OUTPUT###", 1)[1]
            code = code.split("###END_OUTPUT###", 1)[0]
        except Exception:
            # if anything unexpected, fall through and let the old logic run
            pass
        code = code.strip()

    if code.startswith("```"):
        code = re.sub(r"^```[a-zA-Z]*", "", code, 1, flags=re.S).strip()
    if code.endswith("```"):
        code = code[:-3]
    return code.strip()

def _meaningful(code: str) -> bool:
    return any(line and not line.lstrip().startswith("--") and not line.lstrip().startswith("#")
               for line in code.splitlines())

# ---------- PYTHON validation ----------------------------------
def _validate_python(code: str) -> Tuple[bool, str]:
    cleaned = _clean(code)
    if not cleaned:
        return False, "Empty code block"
    if not _meaningful(cleaned):
        return False, "Only comments or empty lines"
    try:
        ast.parse(cleaned)
        return True, "Valid Python syntax"
    except SyntaxError as e:
        return False, f"SyntaxError: {e.msg} on line {e.lineno}"

# ---------- rudimentary SQL validation -------------------------
def _balanced(s: str, open_ch: str = "(", close_ch: str = ")") -> bool:
    cnt = 0
    for ch in s:
        if ch == open_ch:  cnt += 1
        if ch == close_ch: cnt -= 1
        if cnt < 0: return False
    return cnt == 0

def _validate_sql(code: str) -> Tuple[bool, str]:
    cleaned = _clean(code)
    if not cleaned:
        return False, "Empty code block"
    if not _meaningful(cleaned):
        return False, "Only comments or empty lines"
    # very lightweight checks
    if not _balanced(cleaned):
        return False, "Unbalanced parentheses"
    if cleaned.count("'") % 2 or cleaned.count('"') % 2:
        return False, "Unbalanced quotes"
    return True, "Looks like valid SQL (heuristic)"

# ----------------------------------------------------------------
def validate_chunk(code: str, target: str) -> Tuple[bool, str]:
    if target in PYTHON_TARGETS:
        return _validate_python(code)
    if target in SQL_TARGETS:
        return _validate_sql(code)
    # fallback: accept
    return True, "No validation rule for target"

# ───────────────────── main node ────────────────────────────────
def validate_node(state: Dict) -> Dict:
    print("✅ Running Validation Node...")

    target = state.get("target", "pyspark").lower()
    chunks: List[Dict] = state.get("pyspark_chunks", [])
    csv_path = Path(state["rule_csv"])

    logs = state.get("logs", [])
    validation_results, failed_chunks = [], []

    for ch in chunks:
        ok, reason = validate_chunk(ch["code"], target)
        validation_results.append({"id": ch["id"], "validated": ok, "reason": reason})
        if not ok:
            failed_chunks.append(ch["id"])

    # update CSV (column names preserved)
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        res_map = {v["id"]: v for v in validation_results}
        df["validated"] = df["id"].map(lambda i: res_map.get(i, {}).get("validated", False))
        df["reason"]    = df["id"].map(lambda i: res_map.get(i, {}).get("reason", "Not validated"))
        df["validation_status"] = df["validated"].map(lambda v: "passed" if v else "validation_failed")
        df.to_csv(csv_path, index=False)

    logs.append(
        f"Validation passed: {len(chunks) - len(failed_chunks)}; "
        f"failed: {len(failed_chunks)}"
    )

    # write failed chunks for feedback agent
    if failed_chunks:
        src_lookup = {b["id"]: b["code"] for b in state.get("ast_blocks", [])}
        failed_data = []
        for ch in chunks:
            if ch["id"] in failed_chunks:
                failed_data.append({
                    "id": ch["id"],
                    "source_code": src_lookup.get(ch["id"], ""),
                    "generated_code": ch["code"],
                    "reason": next(r["reason"] for r in validation_results if r["id"] == ch["id"])
                })
        Path("rule_outputs").mkdir(exist_ok=True)
        with open(Path("rule_outputs") / "invalid_chunks_for_feedback.json", "w", encoding="utf-8") as f:
            json.dump(failed_data, f, indent=2)

    trace = state.get("graph_trace", [])
    trace.append("validate")

    return {
        **state,
        "validation_passed": len(failed_chunks) == 0,
        "logs": logs,
        "token_usage": state.get("token_usage"),
        "graph_trace": trace
    }





# # backend/agents/validate_agent.py
# from __future__ import annotations
# import ast
# import pandas as pd
# from pathlib import Path
# from typing import Dict, List, Tuple
# import json  # ✅ new import for saving failed chunks to feedback file

# def clean_code(code: str) -> str:
#     code = code.strip()
#     if code.startswith("```python"):
#         code = code[9:]
#     if code.endswith("```"):
#         code = code[:-3]
#     return code.strip()

# def is_meaningful_code(code: str) -> bool:
#     lines = [line.strip() for line in code.splitlines()]
#     return any(line and not line.startswith("#") for line in lines)

# def validate_chunk(code: str) -> Tuple[bool, str]:
#     cleaned = clean_code(code)
#     if not cleaned:
#         return False, "Empty code block"
#     if not is_meaningful_code(cleaned):
#         return False, "Only comments or empty lines"

#     try:
#         ast.parse(cleaned)
#         return True, "Valid Python syntax"
#     except SyntaxError as e:
#         return False, f"SyntaxError: {e.msg} on line {e.lineno}"

# def validate_node(state: Dict) -> Dict:
#     print("✅ Running Validation Node...")

#     chunks: List[Dict] = state.get("pyspark_chunks", [])
#     csv_path = Path(state["rule_csv"])
#     logs = state.get("logs", [])

#     validation_results = []
#     failed_chunks = []

#     for chunk in chunks:
#         ok, reason = validate_chunk(chunk["code"])
#         validation_results.append({
#             "id": chunk["id"],
#             "validated": ok,
#             "reason": reason
#         })
#         if not ok:
#             failed_chunks.append(chunk["id"])

#     # Update same CSV with validation results
#     if csv_path.exists():
#         df = pd.read_csv(csv_path)
#         result_map = {v["id"]: v for v in validation_results}

#         df["validated"] = df["id"].map(lambda i: result_map.get(i, {}).get("validated", False))
#         df["reason"] = df["id"].map(lambda i: result_map.get(i, {}).get("reason", "Not validated"))
#         df["validation_status"] = df["id"].map(
#             lambda i: "passed" if result_map[i]["validated"] else "validation_failed"
#         )

#         df.to_csv(csv_path, index=False)

#     logs.append(f"Validation passed: {len(chunks) - len(failed_chunks)} chunks; failed: {len(failed_chunks)}")

#     # ✅ Write failed chunks to JSON for feedback agent
#     if failed_chunks:
#         sas_lookup = {b["id"]: b["code"] for b in state.get("ast_blocks", [])}
#         failed_data = []
#         for chunk in chunks:
#             if chunk["id"] in failed_chunks:
#                 failed_data.append({
#                     "id": chunk["id"],
#                     "sas_code": sas_lookup.get(chunk["id"], ""),
#                     "pyspark_code": chunk["code"],
#                     "reason": next((r["reason"] for r in validation_results if r["id"] == chunk["id"]), "")
#                 })
#         feedback_path = Path("rule_outputs") / "invalid_chunks_for_feedback.json"
#         with open(feedback_path, "w", encoding="utf-8") as f:
#             json.dump(failed_data, f, indent=2)

#     trace = state.get("graph_trace", [])
#     trace.append("validate")

#     return {
#         **state,
#         "validation_passed": len(failed_chunks) == 0,
#         "logs": logs,
#         "token_usage": state.get("token_usage"),
#         "graph_trace": trace
#     }
