# backend/quick_test_router.py

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from db import engine
from models.llm_credential import LLMCredential
from agents.router_crew import build_router_crew


async def test_router_agent(file_path, cred_id=1):
    with open(file_path, 'r') as f:
        sas_code = f.read()

    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        cred = await session.get(LLMCredential, cred_id)
        if not cred:
            print(f"❌ No credential found for ID {cred_id}")
            return

    crew = build_router_crew(cred, sas_code)
    result = crew.run()
    print("\n🎯 Final Output:\n", result)


if __name__ == "__main__":
    asyncio.run(test_router_agent("sample.sas", cred_id=1))
