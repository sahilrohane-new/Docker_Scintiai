# from fastapi import APIRouter, UploadFile, Form, Depends, HTTPException, Body
# from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
# from sqlalchemy import select
# from sqlalchemy.ext.asyncio import AsyncSession
# import tempfile, os, json, ast
# from pathlib import Path

# from db import get_session
# from models.llm_credential import LLMCredential
# from dependencies.auth_dependencies import get_current_user
# from tasks.conversion_runner import submit_job, get_job, stop_job

# router = APIRouter(tags=["Conversion"])

# # ─────────────────────────── 1. convert
# @router.post("/convert")
# async def convert(
#     file: UploadFile,
#     llm_cred_id: int = Form(...),
#     session: AsyncSession = Depends(get_session),
#     current_user  = Depends(get_current_user),
# ):
#     if not file.filename.lower().endswith(".sas"):
#         raise HTTPException(400, "Only .sas files are supported.")
#     sas_code = (await file.read()).decode("utf-8", errors="ignore")

#     cred: LLMCredential|None = (
#         await session.execute(
#             select(LLMCredential).where(
#                 LLMCredential.id == llm_cred_id,
#                 LLMCredential.user_id == current_user.id
#             )
#         )
#     ).scalar_one_or_none()
#     if not cred: raise HTTPException(404, "Credential not found")

#     state = {
#         "sas_code": sas_code,
#         "llm_provider": cred.provider,
#         "llm_cred": {
#             "openai_api_base": cred.openai_api_base,
#             "openai_api_key":  cred.openai_api_key,
#             "openai_api_version": cred.openai_api_version,
#             "deployment_name": cred.deployment_name,
#             "model_name": cred.model_name,
#             "google_api_key": cred.google_api_key,
#         },
#         "logs": [],
#     }
#     return {"job_id": submit_job(state)}

# # ─────────────────────────── 2. estimate cost  (unchanged)
# # ────────────────────────────────  2. Estimate cost
# @router.post("/estimate_cost")
# async def estimate_cost(
#     file: UploadFile,
#     llm_cred_id: int = Form(...),
#     session: AsyncSession = Depends(get_session),
#     current_user        = Depends(get_current_user),
# ):
#     if not file.filename.lower().endswith(".sas"):
#         raise HTTPException(400, "Only .sas files are supported.")
#     sas_code = (await file.read()).decode("utf-8", errors="ignore")

#     cred: LLMCredential | None = (
#         await session.execute(
#             select(LLMCredential).where(
#                 LLMCredential.id == llm_cred_id,
#                 LLMCredential.user_id == current_user.id,
#             )
#         )
#     ).scalar_one_or_none()
#     if cred is None:
#         raise HTTPException(404, "Credential not found")

#     model_name = cred.model_name.lower()
#     line_cnt   = sas_code.count("\n") + 1
#     # chars_per_line = 42
#     # chars_per_tok  = 4.2

#     # est_llm_in  = int(line_cnt * chars_per_line / chars_per_tok)
#     # est_llm_out = int(est_llm_in * 1.2)
#     # est_opt_in  = int(est_llm_out * .30)
#     # est_opt_out = int(est_opt_in  * .60)

#     # total_in  = est_llm_in + est_opt_in
#     # total_out = est_llm_out + est_opt_out
#     # total_tok = total_in + total_out
#     TOKENS_PER_LINE   = 17.5     # empirically ~17–18
#     LLM_OUT_RATIO     = 0.12     # out ≈12 % of in
#     OPT_IN_RATIO      = 0.04     # optimizer prompt ≈ 4 % of LLM in
#     OPT_OUT_RATIO     = 0.60     # optimizer completion ≈ 60 % of its prompt

#     est_llm_in  = int(line_cnt * TOKENS_PER_LINE)
#     est_llm_out = int(est_llm_in * LLM_OUT_RATIO)
#     est_opt_in  = int(est_llm_in * OPT_IN_RATIO)
#     est_opt_out = int(est_opt_in  * OPT_OUT_RATIO)

#     total_in  = est_llm_in + est_opt_in
#     total_out = est_llm_out + est_opt_out
#     total_tok = total_in + total_out

#     rates = {
#         "gpt-4o": {"in":0.005,"out":0.015},
#         "gpt-4":  {"in":0.03, "out":0.06 },
#         "gpt-35": {"in":0.001,"out":0.002},
#         "gemini": {"in":0.0015,"out":0.0015},
#     }
#     key  = next((k for k in rates if k in model_name), "gpt-4o")
#     cost = round((total_in/1e3)*rates[key]["in"] + (total_out/1e3)*rates[key]["out"], 6)

#     return {
#         "model": model_name,
#         "llm_tokens": {"input": est_llm_in, "output": est_llm_out},
#         "optimize_tokens": {"input": est_opt_in, "output": est_opt_out},
#         "total_tokens": total_tok,
#         "estimated_cost_usd": cost,
#     }


# backend/routers/agent_manager.py
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Body, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
import tempfile, os, json, ast
from pathlib import Path
import io

from db import get_session
from models.llm_credential import LLMCredential
from dependencies.auth_dependencies import get_current_user
from tasks.conversion_runner import submit_job, get_job, stop_job

router = APIRouter(tags=["Conversion"])
PYTHON_TARGETS = {"pyspark", "snowpark","python"}
MAT_TARGETS    = {"matillion"}
DBT_TARGETS    = {"dbt"}
# ─────────────────────────── 1. convert ────────────────────────────
@router.post("/convert")
async def convert(
    request: Request,
    file: UploadFile = File(...),
    llm_cred_id : int   = Form(...),
    source      : str   = Form(...),   # ▼ new
    ddl_type    : str   = Form(...),   # ▼ new
    target      : str   = Form(...),   # ▼ new
    session: AsyncSession = Depends(get_session),
    current_user          = Depends(get_current_user),
):
    form_dump = await request.form()
    print("DEBUG form parts:", {k: type(v).__name__ for k, v in form_dump.items()})
    # --- basic guard -------------------------------------------------
    if source.lower() == target.lower():
        raise HTTPException(400, "Source and Target cannot be the same")

    # (keep the old .sas guard for SAS only)
    if source.lower() == "sas" and not file.filename.lower().endswith(".sas"):
        raise HTTPException(400, "SAS source requires a .sas file")

    code_text = (await file.read()).decode("utf-8", errors="ignore")

    # ------- fetch credential (unchanged) ---------------------------
    cred: LLMCredential | None = (
        await session.execute(
            select(LLMCredential).where(
                LLMCredential.id == llm_cred_id,
                LLMCredential.user_id == current_user.id,
            )
        )
    ).scalar_one_or_none()
    if not cred:
        raise HTTPException(404, "Credential not found")

    orig_name = file.filename
    print("orig_name is: ",orig_name)
    base_name = Path(orig_name).stem
    print("base_name is: ", base_name)
    # ------- seed job-state ----------------------------------------
    state = {
        "sas_code": code_text,          # 🔒 keep key-name for down-stream compat
        "source":   source.lower(),
        "ddl_type": ddl_type.lower(),
        "target":   target.lower(),
        "input_filename": orig_name,
        "input_basename": base_name,

        "llm_provider": cred.provider,
        "llm_cred": {
            "openai_api_base":    cred.openai_api_base,
            "openai_api_key":     cred.openai_api_key,
            "openai_api_version": cred.openai_api_version,
            "deployment_name":    cred.deployment_name,
            "model_name":         cred.model_name,
            "google_api_key":     cred.google_api_key,
        },
        "logs": [],
    }
    print("SOURCE/TARGET/DDL:", source, target, ddl_type)
    return {"job_id": submit_job(state)}

# ─────────────────────────── 2. estimate cost ──────────────────────
@router.post("/estimate_cost")
async def estimate_cost(
    file: UploadFile = File(...),
    llm_cred_id : int   = Form(...),
    source      : str   = Form(...),   # ▼ new
    ddl_type    : str   = Form(...),   # ▼ new
    target      : str   = Form(...),   # ▼ new
    session: AsyncSession = Depends(get_session),
    current_user          = Depends(get_current_user),
):

    if source.lower() == target.lower():
        raise HTTPException(400, "Source and Target cannot be the same")

    if source.lower() == "sas" and not file.filename.lower().endswith(".sas"):
        raise HTTPException(400, "SAS source requires a .sas file")

    code_text = (await file.read()).decode("utf-8", errors="ignore")

    cred: LLMCredential | None = (
        await session.execute(
            select(LLMCredential).where(
                LLMCredential.id == llm_cred_id,
                LLMCredential.user_id == current_user.id,
            )
        )
    ).scalar_one_or_none()
    if cred is None:
        raise HTTPException(404, "Credential not found")

    # ---------- token maths (unchanged logic) -----------------------
    model_name = cred.model_name.lower()
    line_cnt   = code_text.count("\n") + 1

    TOKENS_PER_LINE = 17.5
    LLM_OUT_RATIO   = 0.12
    OPT_IN_RATIO    = 0.04
    OPT_OUT_RATIO   = 0.60

    est_llm_in  = int(line_cnt * TOKENS_PER_LINE)
    est_llm_out = int(est_llm_in * LLM_OUT_RATIO)
    est_opt_in  = int(est_llm_in * OPT_IN_RATIO)
    est_opt_out = int(est_opt_in  * OPT_OUT_RATIO)

    total_in  = est_llm_in + est_opt_in
    total_out = est_llm_out + est_opt_out
    total_tok = total_in + total_out

    rates = {
        "gpt-4o": {"in":0.005,"out":0.015},
        "gpt-4":  {"in":0.03, "out":0.06 },
        "gpt-35": {"in":0.001,"out":0.002},
        "gemini": {"in":0.0015,"out":0.0015},
    }
    key  = next((k for k in rates if k in model_name), "gpt-4o")
    cost = round((total_in/1e3)*rates[key]["in"] + (total_out/1e3)*rates[key]["out"], 6)

    return {
        "model": model_name,
        "llm_tokens":      {"input": est_llm_in, "output": est_llm_out},
        "optimize_tokens": {"input": est_opt_in, "output": est_opt_out},
        "total_tokens":    total_tok,
        "estimated_cost_usd": cost,
    }


# ─────────────────────────── 3. status
@router.get("/status/{job_id}")
async def status(job_id: str):
    j = get_job(job_id)
    if not j: raise HTTPException(404, "Job not found")
    return {
        "job_id":   job_id,
        "status":  j["status"],
        "success": j.get("success", True),
        "logs":    j["logs"],
        "download": j["download"],
        "report":  j["report"],
        "error":   j["error"],
    }

# ─────────────────────────── 4. download
@router.get("/download/{fname}")
async def download_file(fname: str):
    path = os.path.join(tempfile.gettempdir(), fname)
    if not os.path.exists(path):
        raise HTTPException(404, "File expired")
    return FileResponse(path, filename=fname, media_type="application/octet-stream")

# the /rule_download/{fname} route you added earlier:
@router.get("/rule_download/{fname}")
async def rule_download(fname: str):
    path = Path("rule_outputs") / fname
    if not path.exists():
        raise HTTPException(404, "File not found")
    
    download_name = fname
    if fname.endswith(".src"):
        if "pyspark" in fname or "snowpark" in fname:
            download_name = fname.replace(".src", ".py")
        else:
            download_name = fname.replace(".src", ".sql")
    return FileResponse(path, filename=download_name, media_type="application/octet-stream")

# ─────────────────────────── 5. report
@router.get("/report/{job_id}")
async def report(job_id: str):
    j = get_job(job_id)
    if not j or not j["report_path"]:
        raise HTTPException(404, "Report not ready")
    return JSONResponse(json.load(open(j["report_path"], encoding="utf-8")))

# ─────────────────────────── 6. force-stop
@router.post("/force_stop/{job_id}")
async def force_stop(job_id: str):
    if stop_job(job_id): return {"stopped": True}
    raise HTTPException(404, "Job not running")

@router.get("/manual_review_chunks")
async def get_manual_review_chunks():
    p = Path("rule_outputs/manual_review_chunks.json")
    return json.load(p.open()) if p.exists() else []

@router.post("/revalidate_chunk")
async def revalidate_chunk(payload: dict = Body(...)):
    code = payload.get("code", "")
    code = code.strip().removeprefix("```python").removesuffix("```")
    try:
        ast.parse(code)
        return {"validated": True, "reason": "Valid Python syntax"}
    except SyntaxError as e:
        return {"validated": False, "reason": f"SyntaxError: {e.msg} line {e.lineno}"}
    
@router.get("/rule_before_py")
async def rule_before_py():
    src = Path("rule_outputs") / "before_optimization.src"
    if not src.exists():
        raise HTTPException(status_code=404, detail="File not found")
    # 👉 serve it *as* a .py
    return FileResponse(src, filename="before_optimization.py",
                        media_type="application/octet-stream")

@router.get("/download_final/{job_id}")
async def download_final(job_id: str):
    j = get_job(job_id)
    print("DEBUG /download_final job?", bool(j), "keys:", list(j.keys()) if j else None)
    if not j:
        raise HTTPException(404, "Job not found")
    st = j.get("state", {})
    print("DEBUG state keys:", st.keys())
    print("DEBUG final_code:", st.get("final_code") is not None,
          "pyspark_code:", st.get("pyspark_code") is not None,
          "target:", st.get("target"))
    code = st.get("final_code") or st.get("pyspark_code")
    if code is None:
        raise HTTPException(404, "Final code not ready")

    target = st.get("target", "pyspark").lower()
    base = st.get("input_basename")
    # ext = ".py" if target in PYTHON_TARGETS else ".sql"
    EXT_BY_TARGET = {
    "pyspark": ".py",
    "python" : ".py",
    "snowpark": ".py",
    "matillion": ".json",
    "dbt": ".yml",
    }
    ext = EXT_BY_TARGET.get(target, ".sql")
    # fname = f"final_optimized_{target}{ext}"
    fname = f"Optimized_{base}{ext}"
    # strip code fences if present
    if code.startswith("```"):
        lines = code.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        code = "\n".join(lines)

    buf = io.BytesIO(code.encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )


@router.get("/download_before/{job_id}")
async def download_before(job_id: str):
    j = get_job(job_id)
    print("DEBUG /download_before job?", bool(j))
    if not j:
        raise HTTPException(404, "Job not found")
    st = j.get("state", {})
    print("DEBUG before_code present?", st.get("before_code") is not None)
    code = st.get("before_code")
    if code is None:
        raise HTTPException(404, "Before-optimization code not ready")

    target = st.get("target", "pyspark").lower()
    base = st.get("input_basename")
    # ext = ".py" if target in PYTHON_TARGETS else ".sql"
    EXT_BY_TARGET = {
    "pyspark": ".py",
    "python" : ".py",
    "snowpark": ".py",
    "matillion": ".json",
    "dbt": ".yml",
    }
    ext = EXT_BY_TARGET.get(target, ".sql")
    fname = f"Before_Optimized_{base}{ext}"

    buf = io.BytesIO(code.encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )
