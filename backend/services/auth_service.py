# backend/services/auth_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from passlib.context import CryptContext
from sqlalchemy.future import select
from jose import jwt, JWTError
from config import settings
from models.user import User
from schemas.user_schema import UserCreate, UserLogin, UserRead
from sqlalchemy import text

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class AuthService:
    @staticmethod
    async def signup(session: AsyncSession, user_data: UserCreate) -> UserRead:
        # Example logic:
        hashed_password = pwd_context.hash(user_data.password)
        new_user = User(
            email=user_data.email,
            hashed_password=hashed_password,
        )
        session.add(new_user)
        await session.commit()
        await session.refresh(new_user)
        return UserRead(id=new_user.id, email=new_user.email)

    @staticmethod
    # async def login(session: AsyncSession, credentials: UserLogin) -> str:
    #     # Lookup user
    #     result = await session.execute(text(
    #         "SELECT * FROM users WHERE email = :email"),
    #         {"email": credentials.email}
    #     )
    #     db_user = result.fetchone()
    #     if not db_user:
    #         return None
    #     user = db_user[0]

    #     if not pwd_context.verify(credentials.password, user.hashed_password):
    #         return None

    #     # Create JWT token
    #     token_data = {"sub": user.email}
    #     token = jwt.encode(token_data, settings.JWT_SECRET_KEY, algorithm=settings.ALGORITHM)
    #     return token
    async def login(session: AsyncSession, credentials: UserLogin) -> str | None:
        # Use ORM-style query to fetch the user
        stmt = select(User).where(User.email == credentials.email)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            return None

        # Verify password
        if not pwd_context.verify(credentials.password, user.hashed_password):
            return None

        # Generate JWT token
        token_data = {"sub": user.email}
        token = jwt.encode(token_data, settings.JWT_SECRET_KEY, algorithm=settings.ALGORITHM)
        return token
