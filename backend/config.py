# backend/config.py

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Some environment variables
    DATABASE_URL: str = "sqlite+aiosqlite:///./test.db"
    JWT_SECRET_KEY: str = "supersecret"
    ALGORITHM: str = "HS256"

    # Add other config variables as needed
    class Config:
        env_file = ".env"

settings = Settings()
