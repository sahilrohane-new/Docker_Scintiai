import os
import uvicorn
from fastapi import FastAPI
from starlette.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from db import init_db
from routers import auth, agent_manager, settings as settings_router


def create_app() -> FastAPI:
    app = FastAPI(title="SAS to PySpark Multi-Agent App")

    # Allow frontend (React) to access APIs on Cloud Run
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://frontend-service-795288902530.us-west1.run.app"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Optional static files mount (keep or remove as needed)
    app.mount("/static", StaticFiles(directory="static"), name="static")

    #Root route for Cloud Run health & testing
    @app.get("/")
    def root():
        return {"message": "Backend is running successfully"}

    # Routers
    app.include_router(auth.router, prefix="/auth", tags=["Auth"])
    app.include_router(agent_manager.router, prefix="/agent", tags=["Agents"])
    app.include_router(settings_router.router, prefix="/settings", tags=["Settings"])

    return app

app = create_app()

@app.on_event("startup")
async def on_startup():
    await init_db()


if __name__ == "__main__":
    # Cloud Run sets $PORT dynamically (default 8080)
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
