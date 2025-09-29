from pydantic import BaseModel
from typing import Optional

class LLMCreate(BaseModel):
    provider: str
    name: str
    OPENAI_API_BASE: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None
    OPENAI_API_VERSION: Optional[str] = None
    DEPLOYMENT_NAME: Optional[str] = None
    MODEL_NAME: Optional[str] = None
    GOOGLE_API_KEY: Optional[str] = None
    replace: bool = False        # front‑end sets true if user confirmed replacement

class LLMRead(BaseModel):
    id: int
    provider: str
    name: str
    class Config:
        orm_mode = True
