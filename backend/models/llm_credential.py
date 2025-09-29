# backend/models/llm_credential.py
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, func
from sqlalchemy.orm import relationship
from .user import Base

class LLMCredential(Base):
    __tablename__ = "llm_credentials"

    id           = Column(Integer, primary_key=True, index=True)
    user_id      = Column(Integer, ForeignKey("users.id"), nullable=False)
    provider     = Column(String, nullable=False)           # "azureopenai" or "gemini"
    name         = Column(String, nullable=False)           # Friendly label shown in UI

    # Azure fields
    openai_api_base     = Column(String)
    openai_api_key      = Column(String)
    openai_api_version  = Column(String)
    deployment_name     = Column(String)
    model_name          = Column(String)

    # Gemini fields
    google_api_key      = Column(String)

    created_at   = Column(DateTime(timezone=True), server_default=func.now())

    owner = relationship("User", back_populates="llm_creds")
