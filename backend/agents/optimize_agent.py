from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime
from time import perf_counter
import re, json, pandas as pd

from langchain_openai       import AzureChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

# ────────────────────────── config ────────────────────────────
PYTHON_TARGETS = {"pyspark", "snowpark","python"}               # code runs in Python VM
SQL_TARGETS    = {"databricks", "snowflake", "bigquery"}
MAT_TARGETS    = {"matillion"}
DBT_TARGETS    = {"dbt"}
ALL_TARGETS    = PYTHON_TARGETS | SQL_TARGETS | MAT_TARGETS | DBT_TARGETS

OUT_DIR          = Path("rule_outputs")
OUT_CSV          = OUT_DIR / "final_optimized_pyspark.csv"    # 📌 kept for backward-compat
DIFF_CSV         = OUT_DIR / "before_after_comparison.csv"
REPORT_JSON_PATH = OUT_DIR / "optimization_report.json"
LLM_TOKEN_JSON   = OUT_DIR / "llm_token_usage.json"

# ────────────────────────── helpers ───────────────────────────
def _build_prompt(target: str) -> ChatPromptTemplate:
    """Return a prompt tailored to the target run-time."""
    # lang = "PySpark" if target.lower() == "pyspark" else (
    #        "Snowpark (Python)" if target.lower() == "snowpark" else "SQL")
    if target.lower()=="pyspark":
        lang = "PySpark"
    
    elif target.lower()=="snowpark":
        lang = "Snowpark (Python)"
    
    elif target.lower()=="matillion":
        lang = "Matillion"
    
    elif target.lower()=="dbt":
        lang = "DBT"
    
    elif target.lower()=="python":
        lang = "Python"

    else:
        lang = "SQL"
    
    print("Optimising for:",lang)
    sys_msg = (
        f"You are a {lang} code optimizer.\n"
        f"Optimize the provided {lang} script while preserving business logic, "
        f"naming conventions and comments. Do **not** remove any functional "
        f"statements or drop any functionality. Produce a single, executable {lang} script as output."
    )
    
    return ChatPromptTemplate.from_messages([
        ("system", sys_msg),
        ("user",   f"Optimize this {lang} code:\n{{code}}")
    ])

def _load_llm(state: Dict):
    cred, provider = state["llm_cred"], state["llm_provider"]
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

def _dedup_python(code: str) -> str:
    """Remove duplicate imports & builder lines – Py* only."""
    seen, cleaned = set(), []
    for ln in code.splitlines():
        t = ln.strip()
        if t.startswith(("import", "from", "spark = SparkSession.builder")):
            if t not in seen:
                seen.add(t); cleaned.append(ln)
        else:
            cleaned.append(ln)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned))

# ────────────────────────── main node ─────────────────────────
def optimize_node(state: Dict) -> Dict:
    t0 = perf_counter()
    print("🧹  Optimizer Node")

    target = state.get("target").lower()
    print("Target is:",target)
    if target not in ALL_TARGETS:
        print("if 1")
        state["logs"].append(f"Unknown target '{target}' – skipping optimizer.")
        return state

    # ▸ 1. token-usage bookkeeping (unchanged) ───────────────────
    tok_usage = state.get("token_usage", {})
    if LLM_TOKEN_JSON.exists():
        tok_usage.update(json.loads(Path(LLM_TOKEN_JSON).read_text()))
    ci = tok_usage.get("llm", {}).get("input", 0)
    co = tok_usage.get("llm", {}).get("output", 0)
    state.setdefault("logs", []).append(f"[llm] tokens in={ci}, out={co}")

    # ▸ 2. merge chunks from previous stage (legacy key kept) ─────
    chunks  = state.get("pyspark_chunks", [])
    merged  = "\n\n".join(c["code"].strip() for c in chunks)
    base_code = (
        _dedup_python(merged) if target in PYTHON_TARGETS else merged.strip()
    )

    OUT_DIR.mkdir(exist_ok=True)
    before_path = OUT_DIR / "before_optimization.src"
    before_path.write_text(base_code, encoding="utf-8")

    # ▸ 3. LLM optimisation run ──────────────────────────────────
    in2 = out2 = 0
    final = base_code
    if base_code:
        try:
            print("try enabled")
            llm    = _load_llm(state)
            prompt = _build_prompt(target).format_prompt(code=base_code).to_messages()
            # print("Prompt is : ", prompt)
            resp   = llm.invoke(prompt)
            final  = resp.content.strip() or base_code
            # print("Final Code :", final)
            if hasattr(resp, "usage"):
                print("Landed in iffff")
                in2, out2 = resp.usage.prompt_tokens, resp.usage.completion_tokens
            state["logs"].append("LLM optimization succeeded.")
        except Exception as e:
            print("Errroooooorrr")
            state["logs"].append(f"LLM optimization error: {e}")
    else:
        state["logs"].append("Optimizer skipped – empty code.")

    if (in2 + out2) == 0 and base_code:
        in2, out2 = len(base_code.split()), len(final.split())
    state["logs"].append(f"[optimize] tokens in={in2}, out={out2}")

    # ▸ 4. update token_usage dict ───────────────────────────────
    tok_usage["optimize"] = {
        "input": in2, "output": out2, "total": in2+out2,
        "model": state["llm_cred"]["model_name"]
    }
    state["token_usage"] = tok_usage

    # ▸ 5. persist legacy CSVs (unchanged) ───────────────────────
    # pd.DataFrame({"pyspark_code": [final]}).to_csv(OUT_CSV, index=False)
    # pd.DataFrame({"before": [base_code], "after": [final]}).to_csv(DIFF_CSV, index=False)

    # ▸ 6. also write final script file for download ─────────────
    # ext = ".src" if target in PYTHON_TARGETS else ".sql"
    EXT_BY_TARGET = {
    "pyspark": ".src",      # keep .src so downstream converts to .py
    "snowpark": ".src",
    "python":  ".src",
    "matillion": ".json",
    "dbt": ".yml",
    }
    ext = EXT_BY_TARGET.get(target, ".sql")
    final_path = OUT_DIR / f"final_optimized_{target}{ext}"
    final_path.write_text(final, encoding="utf-8")

    # ▸ 7. cost maths (kept) ─────────────────────────────────────
    ti = sum(d["input"]  for d in tok_usage.values())
    to = sum(d["output"] for d in tok_usage.values())
    RATES = {
        "gpt-4o": {"input":0.005,"output":0.015},
        "gpt-4":  {"input":0.03, "output":0.06 },
        "gpt-35": {"input":0.001,"output":0.002},
        "gemini": {"input":0.0015,"output":0.0015}
    }
    mdl = state["llm_cred"]["model_name"].lower()
    rate= next((v for k,v in RATES.items() if k in mdl), RATES["gpt-4o"])
    cost= round((ti/1e3)*rate["input"] + (to/1e3)*rate["output"], 6)
    dt  = round(perf_counter()-t0,2)
    state["before_code"] = base_code          # raw merged pre-LLM code
    state["final_code"]  = final              # optimized code (possibly python/sql)

    orig_name = state.get("input_filename")
    print("optimizeagent origname:",orig_name)
    base_name = state.get("input_basename") or Path(orig_name).stem
    print("optimizeagent base name:", base_name)

    # ▸ 8. report json (added optimized_file key) ────────────────
    report = {
        "summary": {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "llm": state["llm_provider"], "model": state["llm_cred"]["model_name"],
            "chunks": len(chunks), "total_cost_usd": cost,
            "target": target,
            "input_filename": orig_name,
            "input_basename": base_name,
        },
        "input": {
            "sas_line_count": state.get("sas_code","").count("\n")+1,
            "chunk_count": len(chunks)
        },
        "optimization": {
            "lines_before": len(base_code.splitlines()),
            "lines_after":  len(final.splitlines())
        },
        "llm_usage": {
            "by_stage": tok_usage, "input_tokens": ti,
            "output_tokens": to, "total_tokens": ti+to,
            "estimated_cost_usd": cost
        },
        "runtime_sec": dt,
        "graph_trace": state.get("graph_trace", []),
        "files": {
            "optimized_code_csv": str(OUT_CSV),
            "diff_csv":           str(DIFF_CSV),
            "before_src":         str(before_path.name),
            "optimized_file":     str(final_path.name)
        }
    }
    REPORT_JSON_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    state["report_file"] = str(REPORT_JSON_PATH.resolve())

    state["validation_passed"] = True
    # ▸ 9. return (legacy keys preserved) ────────────────────────
    return {
        **state,
        "pyspark_code": final,          # ⚠️ kept for backward compatibility
        "before_code" : base_code,
        "optimized_file": str(final_path),
        "logs": state["logs"],
        "report": report,
        "runtime": dt,
        "token_usage": tok_usage,
        "graph_trace": state.get("graph_trace", []) + ["optimize"]
    }





# from __future__ import annotations
# from typing import Dict, List
# from pathlib import Path
# from datetime import datetime
# import re, json, pandas as pd
# from time import perf_counter

# from langchain_openai       import AzureChatOpenAI
# from langchain_google_genai import ChatGoogleGenerativeAI
# from langchain_core.prompts import ChatPromptTemplate

# OUT_DIR          = Path("rule_outputs")
# OUT_CSV          = OUT_DIR / "final_optimized_pyspark.csv"
# DIFF_CSV         = OUT_DIR / "before_after_comparison.csv"
# REPORT_JSON_PATH = OUT_DIR / "optimization_report.json"
# LLM_TOKEN_JSON   = OUT_DIR / "llm_token_usage.json"  # ← 🔁 added

# PROMPT = ChatPromptTemplate.from_messages([
#     ("system",
#      """You are a PySpark code optimizer.Optimize the provided PySpark code while preserving its original business logic and naming conventions.
#      Make sure you retain all code blocks even if different values are assigned to same variables.Avoid duplicate imports.
#      Provide a single working PySpark code block as output."""),
#     ("user",
#      "Optimize this PySpark code:\n{code}")
# ])

# # ───────────────────────── helpers ─────────────────────────────
# def _load_llm(state: Dict):
#     cred, provider = state["llm_cred"], state["llm_provider"]
#     if provider == "azureopenai":
#         return AzureChatOpenAI(
#             azure_endpoint     = cred["openai_api_base"],
#             openai_api_key     = cred["openai_api_key"],
#             openai_api_version = cred["openai_api_version"],
#             deployment_name    = cred["deployment_name"],
#             model_name         = cred["model_name"],
#             temperature        = 0.0,
#         )
#     if provider == "gemini":
#         return ChatGoogleGenerativeAI(
#             model          = cred["model_name"],
#             google_api_key = cred["google_api_key"], 
#             temperature    = 0.0,
#         )
#     raise ValueError("Unsupported provider")

# # ───────────────────────── main node ──────────────────────────
# def optimize_node(state: Dict) -> Dict:
#     t0 = perf_counter()
#     print("🧹  Optimizer Node") 

#     # 1️⃣ Load LLM token usage from file if available
#     tok_usage = state.get("token_usage", {})

#     if LLM_TOKEN_JSON.exists():
#         with open(LLM_TOKEN_JSON, "r") as f:
#             llm_tok = json.load(f)
#             tok_usage.update(llm_tok)

#     llm_stage = tok_usage.get("llm", {"input": 0, "output": 0, "total": 0, "model": ""})
#     ci, co    = llm_stage["input"], llm_stage["output"]
#     state["logs"].append(f"[llm] tokens in={ci}, out={co}")

#     # 2️⃣ merge & deduplicate code
#     chunks  = state.get("pyspark_chunks", [])
#     merged  = "\n\n".join(c["code"].strip() for c in chunks)
#     seen, cleaned = set(), []
#     for ln in merged.splitlines():
#         t = ln.strip()
#         if t.startswith(("import","from","spark = SparkSession.builder")):
#             if t not in seen:
#                 seen.add(t); cleaned.append(ln)
#         else:
#             cleaned.append(ln)
#     base_code = re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned))
#     # --- write "before" code without .py extension -----------------
#     before_path = OUT_DIR / "before_optimization.src"   # ⚠️  not .py
#     before_path.write_text(base_code.strip(), encoding="utf-8")



#     # Save the unoptimized PySpark code to .py file for download
#     print(f"🔍 Merged code: {base_code.strip()}")

#     # 3️⃣ run optimizer LLM
#     in2 = out2 = 0
#     try:
#         if base_code.strip():
#             llm    = _load_llm(state)
#             prompt = PROMPT.format_prompt(code=base_code).to_messages()
#             resp   = llm.invoke(prompt)
#             final  = resp.content.strip()
#             print(f"🔍 Optimized code: {final}")
#             if hasattr(resp, "usage"):
#                 in2, out2 = resp.usage.prompt_tokens, resp.usage.completion_tokens
#             state["logs"].append("LLM optimization succeeded.")
#         else:
#             final = base_code
#             state["logs"].append("Optimizer skipped (empty code).")
#     except Exception as e:
#         final = base_code
#         state["logs"].append(f"LLM optimization error: {e}")

#     if (in2 + out2) == 0 and base_code:
#         in2, out2 = len(base_code.split()), len(final.split())
#     state["logs"].append(f"[optimize] tokens in={in2}, out={out2}")

#     # 4️⃣ update token_usage dict
#     tok_usage["optimize"] = {
#         "input": in2,
#         "output": out2,
#         "total": in2 + out2,
#         "model": state["llm_cred"]["model_name"]
#     }
#     state["token_usage"] = tok_usage

#     # 5️⃣ persist CSVs
#     OUT_DIR.mkdir(exist_ok=True)
#     pd.DataFrame({"pyspark_code":[final]}).to_csv(OUT_CSV, index=False)
#     pd.DataFrame({"before":[base_code], "after":[final]}).to_csv(DIFF_CSV, index=False)
    

#     # 6️⃣ aggregate totals & cost
#     ti  = sum(d["input"]  for d in tok_usage.values())
#     to  = sum(d["output"] for d in tok_usage.values())

#     tt = ti + to

#     # Directional pricing per model
#     RATES = {
#         "gpt-4o":  {"input": 0.005,  "output": 0.015},
#         "gpt-4":   {"input": 0.03,   "output": 0.06},
#         "gpt-35":  {"input": 0.001,  "output": 0.002},
#         "gemini":  {"input": 0.0015, "output": 0.0015}
#     }
#     mdl = state["llm_cred"]["model_name"].lower()
#     matched = next((k for k in RATES if k in mdl), "gpt-4o")
#     model_rate = RATES[matched]

#     cost = round((ti / 1000 * model_rate["input"]) + (to / 1000 * model_rate["output"]), 6)


#     dt = round(perf_counter() - t0, 2)

#     # 7️⃣ build report
#     report = {
#         "summary": {
#             "timestamp":     datetime.now().isoformat(timespec="seconds"),
#             "llm":           state["llm_provider"],
#             "model":         state["llm_cred"]["model_name"],
#             "chunks":        len(chunks),
#             "total_cost_usd": cost
#         },
#         "input": {
#             "sas_line_count": state.get("sas_code", "").count("\n") + 1,
#             "chunk_count":    len(chunks)
#         },
#         "optimization": {
#             "lines_before": len(base_code.splitlines()),
#             "lines_after":  len(final.splitlines())
#         },
#         "llm_usage": {
#             "by_stage": tok_usage,
#             "input_tokens":  ti,
#             "output_tokens": to,
#             "total_tokens":  tt,
#             "estimated_cost_usd": cost
#         },
#         "runtime_sec": dt,
#         "graph_trace": state.get("graph_trace", []),
#         "files": {
#     "optimized_code_csv": str(OUT_CSV),
#     "diff_csv":           str(DIFF_CSV),
#     "before_src":         str(before_path.name)

# }

#     }
#     with REPORT_JSON_PATH.open("w", encoding="utf-8") as f:
#         json.dump(report, f, indent=2)

#     abs_report = str(Path(REPORT_JSON_PATH).resolve())     # ① absolute
#     state["report_file"] = abs_report                      # ② always set

#     return {
#         **state,
#         "pyspark_code": final,
#         "logs":         state["logs"],
#         "report":       report,
#         "runtime":      dt,
#         "token_usage":  tok_usage,
#         "report_file":  abs_report,                # ★ absolute
#     }

