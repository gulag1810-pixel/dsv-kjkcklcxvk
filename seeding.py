import faker
from sqlalchemy.ext.asyncio import AsyncSession

async def user_seeding(session: AsyncSession, amount):
    new_users = []

    for i in range(amount):
        username = faker.name()
        user_email = faker.email()

        new_users.append(User(username=username, user_email=user_email))
    
    session.add_all(new_users)

    await session.commit() 