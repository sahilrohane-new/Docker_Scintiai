# backend/routers/auth.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from dependencies.auth_dependencies import get_current_user
from schemas.user_schema import UserCreate, UserLogin, UserRead
from services.auth_service import AuthService
from db import get_session
from models.user import User

router = APIRouter()

@router.post("/signup", response_model=UserRead)
async def signup(
    user_data: UserCreate,
    session: AsyncSession = Depends(get_session),
):
    try:
        user = await AuthService.signup(session, user_data)
        return user
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/login")
async def login(
    credentials: UserLogin,
    session: AsyncSession = Depends(get_session),
):
    token = await AuthService.login(session, credentials)
    if not token:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"access_token": token, "token_type": "bearer"}

@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {"email": current_user.email}
