# backend/quick_test_parse.py

import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from db import engine
from models.llm_credential import LLMCredential
from agents.utils.sas_chunker import chunk_sas_code
from backend.agents.parse_agent import parse_chunk_simple


async def test_parse_sas_file(file_path, cred_id=1):
    # Read input file
    with open(file_path, 'r') as f:
        sas_code = f.read()

    # Chunk
    chunks = chunk_sas_code(sas_code, max_lines=500)
    print(f"✅ Total Chunks: {len(chunks)}")

    # Get a sample credential (for future RuleCrew/LLMCrew steps)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with async_session() as session:
        cred = await session.get(LLMCredential, cred_id)
        if not cred:
            print(f"❌ No credential found for id {cred_id}")
            return

    # Parse each chunk using deterministic method
    ast_blocks = []
    for i, chunk in enumerate(chunks, start=1):
        print(f"\n🔸 Parsing chunk {i}")
        ast = parse_chunk_simple(chunk)
        print(f"  AST Type: {ast['type']}")
        ast_blocks.append(ast)

    print(f"\n✅ All chunks parsed. Total: {len(ast_blocks)}")

if __name__ == "__main__":
    asyncio.run(test_parse_sas_file("sample.sas", cred_id=1))
