from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from models import User
from faker import Faker
faker = Faker()  

'''
data structure:
{
    "username": string,
    "user_email": string
}
'''

async def user_create(session: AsyncSession, data: dict):
    new_user = User(
        username=data['username'],
        user_email=data['user_email']
    )
    session.add(new_user)
    
    await session.flush() 
    await session.refresh(new_user) 
    
    return new_user

async def user_delete(session: AsyncSession, user_id: int):
    stmt = delete(User).where(User.user_id == user_id)
    await session.execute(stmt)

async def user_get(session: AsyncSession, user_id: int):
    stmt = select(User).where(User.user_id == user_id)
    result = await session.execute(stmt)
    
    selected_user = result.scalars().first()
    return selected_user

async def get_all(session: AsyncSession):
    stmt = select(User)
    result = await session.execute(stmt)
    users = result.scalars().all()
    return users

async def delete_by_list(session: AsyncSession, emails: str):

    stmt = delete(User).where(User.user_email.in_(emails)).returning(User.user_email)
    result = await session.execute(stmt)
    
    return result.scalars().all()
