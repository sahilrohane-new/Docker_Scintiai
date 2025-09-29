# backend/test_graph.py

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from db import get_session
from models.llm_credential import LLMCredential
from graph.main_graph import build_graph


async def load_llm_cred(session, cred_id=1):
    result = await session.execute(
        select(LLMCredential).where(LLMCredential.id == cred_id)
    )
    cred = result.scalar_one_or_none()
    if not cred:
        raise ValueError("❌ Credential not found in DB")
    return cred



async def test_full_pipeline(file_path, cred_id=1):
    async for session in get_session():
        cred = await load_llm_cred(session, cred_id)

        # Read test .sas file
        with open(file_path, "r") as f:
            sas_code = f.read()

        graph = build_graph()

        # Prepare input state
        input_state = {
            "sas_code": sas_code,
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

        print("🚀 Starting LangGraph agent pipeline...\n")
        final_state = graph.invoke(input_state)

        print("🧾 Final Logs:")
        for log in final_state["logs"]:
            print("•", log)

        print("\n📄 Final PySpark Code:\n", final_state.get("pyspark_code"))


if __name__ == "__main__":
    asyncio.run(test_full_pipeline("sample.sas", cred_id=1))
