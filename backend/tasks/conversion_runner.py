# backend/tasks/conversion_runner.py
import uuid, asyncio, tempfile, os, traceback
from pathlib import Path
from time import perf_counter
from graph.main_graph import build_graph
from langgraph.errors import GraphRecursionError

JOBS: dict[str, dict] = {}

# ── helpers ───────────────────────────────────────────────────
def _init(job_id: str):
    JOBS[job_id] = {
        "status":  "queued",
        "step":    "waiting",
        "progress": 0,
        "current_agent": "waiting",
        "logs":    [],
        "download": "",
        "report":   "",
        "report_path": "",
        "success": None,
        "error":   "",
        "force_stop": False,
    }

def submit_job(state_in: dict) -> str:
    job_id = uuid.uuid4().hex
    _init(job_id)
    asyncio.create_task(_run_job(job_id, {**state_in, "job_id": job_id}))
    return job_id

def get_job(job_id: str) -> dict | None:
    return JOBS.get(job_id)

def stop_job(job_id: str) -> bool:
    j = JOBS.get(job_id)
    if j and j["status"] == "running":
        j.update(force_stop=True, status="stopped", step="stopped")
        j["logs"].append("❌ force-stop requested")
        return True
    return False

# ── main runner ───────────────────────────────────────────────
async def _run_job(job_id: str, state: dict):
    print("DEBUG state keys:", state.keys())
    print("DEBUG state values:", state.get("source"), state.get("target"), state.get("ddl_type"))
    job = JOBS[job_id]
    job["status"] = "running"
    total_steps = 6  # keeps old progress logic
    cur = 0

    try:
        graph = build_graph().with_config(recursion_limit=50)

        async for st in graph.astream(state):
            if job["force_stop"]:
                raise RuntimeError("Force-stop")
            cur += 1
            step = st.get("graph_trace", ["-"])[-1]
            job.update(
                step=step,
                current_agent=step,
                progress=min(99, int(cur / total_steps * 100)),
                logs=st.get("logs", job["logs"]),
            )
            state = st

        # ✅ Unwrap if graph returned { "optimize": {...} }
        if isinstance(state, dict) and "optimize" in state and isinstance(state["optimize"], dict):
            inner = state["optimize"]
            # Only unwrap if the inner dict has the fields we need
            if inner.get("final_code") or inner.get("pyspark_code") or inner.get("before_code"):
                state = inner
        
        job["state"] = state

        # ───────────────────────────────────────────────────────
        # choose proper file-extension based on target type
        # ───────────────────────────────────────────────────────
        target = (state.get("target") or "pyspark").lower()
        ext_map = {
            "pyspark":    ".py",
            "snowpark":   ".py",
            "python"  :   ".py",
            "databricks": ".sql",
            "snowflake":  ".sql",
            "bigquery":   ".sql",
            "matillion":  ".json",
            "dbt"      :  ".yml",
        }
        ext       = ext_map.get(target, ".txt")
        file_name = f"{target}_{job_id}{ext}"
        file_path = os.path.join(tempfile.gettempdir(), file_name)

        # ── merged code string (same fallback logic) ------------
        code_str = state.get("final_code") or state.get("pyspark_code") or ""
        if not code_str:
            csv_fallback = Path("rule_outputs/final_optimized_pyspark.csv")
            if csv_fallback.exists():
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_fallback)
                    code_str = "\n".join(df.iloc[:, 0].astype(str).tolist())
                except Exception:
                    code_str = "# (unable to load fallback CSV)"

        with open(file_path, "w", encoding="utf-8") as fh:
            fh.write(code_str)

        # ── ensure report path ---------------------------------
        rpt_path = state.get("report_file") or ""
        if not rpt_path or not Path(rpt_path).exists():
            default_path = Path("rule_outputs/optimization_report.json").resolve()
            if default_path.exists():
                rpt_path = str(default_path)
        if not Path(rpt_path).exists():
            raise RuntimeError("optimisation report not written")

        job["report_path"] = rpt_path
        job["report"]      = f"/agent/report/{job_id}"

        # ── final success update -------------------------------
        job.update(
            status="finished",
            step="done",
            progress=100,
            success=state.get("validation_passed", True),
            download=f"/agent/download/{file_name}",
        )

    except RuntimeError as e:
        job.update(status="failed", error=str(e), progress=100)
    except GraphRecursionError:
        job.update(status="failed", error="graph recursion limit", progress=100)
    except Exception as exc:
        tb = traceback.format_exc(limit=4).splitlines()[-1]
        job.update(status="failed", error=f"{exc} | {tb}", progress=100)
