# backend/routers/settings.py

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from db import get_session
from models.llm_credential import LLMCredential
from models.user import User
from schemas.llm_schema import LLMCreate, LLMRead
from dependencies.auth_dependencies import get_current_user
from langchain.chat_models import AzureChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI

router = APIRouter()

# ----- Add New / Validate Credential --------------------
@router.post("/llm", response_model=LLMRead)
async def add_llm_cred(
    payload: LLMCreate,
    db: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user)
):
    provider = payload.provider.lower()

    # ---- NEW RULE: Only 1 credential per provider per user ----
    stmt = select(LLMCredential).where(
        LLMCredential.user_id == user.id,
        LLMCredential.provider == provider
    )
    existing = (await db.execute(stmt)).scalars().all()

    if existing and not payload.replace:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="limit_per_provider"
        )
    if existing and payload.replace:
        await db.execute(
            delete(LLMCredential).where(LLMCredential.id == existing[0].id)
        )
        await db.commit()

    # ---- Validate live by calling the LLM ---------------------
    try:
        if provider == "azureopenai":
            _ = AzureChatOpenAI(
                openai_api_base    = payload.OPENAI_API_BASE,
                openai_api_key     = payload.OPENAI_API_KEY,
                openai_api_version = payload.OPENAI_API_VERSION,
                deployment_name    = payload.DEPLOYMENT_NAME,
                model_name         = payload.MODEL_NAME,
                temperature        = 0
            ).invoke("hi")

        elif provider == "gemini":
            _ = ChatGoogleGenerativeAI(
                model          = payload.MODEL_NAME,
                google_api_key = payload.GOOGLE_API_KEY,
                temperature    = 0
            ).invoke("hi")

        else:
            raise HTTPException(400, "unsupported_provider")

    except Exception as e:
        raise HTTPException(400, f"validation_error: {str(e)}")

    # ---- Save in database after validation --------------------
    cred = LLMCredential(
        user_id            = user.id,
        provider           = provider,
        name               = payload.name,
        openai_api_base    = payload.OPENAI_API_BASE,
        openai_api_key     = payload.OPENAI_API_KEY,
        openai_api_version = payload.OPENAI_API_VERSION,
        deployment_name    = payload.DEPLOYMENT_NAME,
        model_name         = payload.MODEL_NAME,
        google_api_key     = payload.GOOGLE_API_KEY,
    )

    db.add(cred)
    await db.commit()
    await db.refresh(cred)
    return cred

# ----- List All Saved Credentials --------------------
@router.get("/llm", response_model=list[LLMRead])
async def list_llm_creds(
    db: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user)
):
    stmt = select(LLMCredential).where(LLMCredential.user_id == user.id)
    rows = (await db.execute(stmt)).scalars().all()
    return rows

# ----- Delete a Specific Credential ------------------
@router.delete("/llm/{cred_id}", status_code=204)
async def delete_llm_cred(
    cred_id: int,
    db: AsyncSession = Depends(get_session),
    user: User = Depends(get_current_user)
):
    stmt = delete(LLMCredential).where(
        LLMCredential.id == cred_id,
        LLMCredential.user_id == user.id
    )
    result = await db.execute(stmt)
    if result.rowcount == 0:
        raise HTTPException(404, "not_found")
    await db.commit()
