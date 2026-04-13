from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from typing import AsyncGenerator
from sqlalchemy.orm import DeclarativeBase
from config import get_db_url

DATABASE_URL = get_db_url()

engine = create_async_engine(DATABASE_URL, echo=True)
session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with session_maker() as session:
        try:
            yield session
            await session.commit() 
        except Exception:
            await session.rollback() 
            raise
        finally:
            await session.close() 

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)