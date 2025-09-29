# backend/main.py

import uvicorn
from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from config import settings
from db import init_db
from routers import auth, agent_manager, settings as settings_router
from fastapi.middleware.cors import CORSMiddleware

def create_app() -> FastAPI:
    app = FastAPI(title="SAS to PySpark Multi-Agent App")
    app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # or ["*"] for all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

    # Mount static files (optional, if you want to serve them from backend)
    # If you're using a separate front-end, you may not need this.
    app.mount("/static", StaticFiles(directory="static"), name="static")

    # Include Routers
    app.include_router(auth.router, prefix="/auth", tags=["Auth"])
    app.include_router(agent_manager.router, prefix="/agent", tags=["Agents"])
    app.include_router(settings_router.router, prefix="/settings", tags=["Settings"])

    return app

app = create_app()

@app.on_event("startup")
async def on_startup():
    # Initialize database (optional)
    await init_db()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
