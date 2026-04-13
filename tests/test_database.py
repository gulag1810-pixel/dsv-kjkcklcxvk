"""
Тесты для проверки функциональности базы данных Telegram-сервиса.
Использует pytest и pytest-asyncio для асинхронных тестов.
"""
import pytest
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select, delete, Integer, String
import re

# Импортируем модели и функции
from models import User, Base
from UserCrud import user_create, user_delete, user_get, get_all, delete_by_list
from core import init_db


# === Конфигурация тестовой БД ===
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/test_telegram_bot"


@pytest.fixture(scope="session")
def event_loop():
    """Создание event loop для сессии тестов."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def test_engine():
    """Создание тестового движка и таблиц."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    
    # Создаем все таблицы из основной модели (включая users)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    # Удаляем таблицы после тестов
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    
    await engine.dispose()


@pytest.fixture(scope="function")
async def session(test_engine):
    """Создание тестовой сессии для каждого теста."""
    async_session_maker = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session_maker() as sess:
        yield sess
        await sess.rollback()  # Откатываем все изменения после теста


# === Тесты модели User (юнит-тесты без БД) ===
class TestUserModelUnit:
    """Тесты валидации модели User."""
    
    def test_email_validation_valid(self):
        """Проверка валидации корректных email адресов."""
        valid_emails = [
            "test@example.com",
            "user.name@domain.org",
            "user+tag@mail.ru",
            "user123@test-domain.co.uk",
            "a@b.co"
        ]
        
        for email in valid_emails:
            assert re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email), \
                f"Email {email} должен быть валидным"
    
    def test_email_validation_invalid(self):
        """Проверка отклонения некорректных email адресов."""
        invalid_emails = [
            "invalid",
            "@example.com",
            "user@",
            "user@domain",
            "",
            "user @example.com"
        ]
        
        for email in invalid_emails:
            assert not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email), \
                f"Email {email} должен быть невалидным"
    
    def test_user_creation(self):
        """Проверка создания объекта User."""
        user = User(username="Test User", user_email="test@example.com")
        
        assert user.username == "Test User"
        assert user.user_email == "test@example.com"
        assert user.user_id is None  # ID устанавливается при сохранении в БД


# === CRUD тесты ===
class TestUserCRUD:
    """Тесты CRUD операций с пользователями."""
    
    @pytest.mark.asyncio
    async def test_user_create(self, session):
        """Тест создания пользователя."""
        user_data = {
            "username": "John Doe",
            "user_email": "john.doe@example.com"
        }
        
        created_user = await user_create(session, user_data)
        
        assert created_user is not None
        assert created_user.username == "John Doe"
        assert created_user.user_email == "john.doe@example.com"
        assert created_user.user_id is not None
    
    @pytest.mark.asyncio
    async def test_user_get(self, session):
        """Тест получения пользователя по ID."""
        # Сначала создаем пользователя
        user_data = {
            "username": "Jane Smith",
            "user_email": "jane.smith@example.com"
        }
        created_user = await user_create(session, user_data)
        await session.commit()
        
        # Получаем пользователя
        retrieved_user = await user_get(session, created_user.user_id)
        
        assert retrieved_user is not None
        assert retrieved_user.user_id == created_user.user_id
        assert retrieved_user.username == "Jane Smith"
        assert retrieved_user.user_email == "jane.smith@example.com"
    
    @pytest.mark.asyncio
    async def test_user_get_not_found(self, session):
        """Тест получения несуществующего пользователя."""
        retrieved_user = await user_get(session, 99999)
        assert retrieved_user is None
    
    @pytest.mark.asyncio
    async def test_user_delete(self, session):
        """Тест удаления пользователя по ID."""
        # Создаем пользователя
        user_data = {
            "username": "Delete Me",
            "user_email": "delete.me@example.com"
        }
        created_user = await user_create(session, user_data)
        await session.commit()
        
        # Удаляем пользователя
        await user_delete(session, created_user.user_id)
        await session.commit()
        
        # Проверяем, что пользователь удален
        retrieved_user = await user_get(session, created_user.user_id)
        assert retrieved_user is None
    
    @pytest.mark.asyncio
    async def test_get_all_users(self, session):
        """Тест получения всех пользователей."""
        # Создаем нескольких пользователей
        users_data = [
            {"username": "User 1", "user_email": "user1@example.com"},
            {"username": "User 2", "user_email": "user2@example.com"},
            {"username": "User 3", "user_email": "user3@example.com"},
        ]
        
        for data in users_data:
            await user_create(session, data)
        await session.commit()
        
        # Получаем всех пользователей
        all_users = await get_all(session)
        
        assert len(all_users) >= 3
        emails = [user.user_email for user in all_users]
        assert "user1@example.com" in emails
        assert "user2@example.com" in emails
        assert "user3@example.com" in emails
    
    @pytest.mark.asyncio
    async def test_get_all_empty(self, session):
        """Тест получения пользователей из пустой таблицы."""
        all_users = await get_all(session)
        assert len(all_users) == 0


# === Тесты массового удаления ===
class TestBulkDelete:
    """Тесты массового удаления пользователей по email."""
    
    @pytest.mark.asyncio
    async def test_delete_by_list_all_exist(self, session):
        """Тест удаления списка существующих пользователей."""
        # Создаем пользователей
        users_data = [
            {"username": "Bulk 1", "user_email": "bulk1@example.com"},
            {"username": "Bulk 2", "user_email": "bulk2@example.com"},
            {"username": "Bulk 3", "user_email": "bulk3@example.com"},
        ]
        
        for data in users_data:
            await user_create(session, data)
        await session.commit()
        
        # Удаляем по списку email
        emails_to_delete = ["bulk1@example.com", "bulk2@example.com"]
        deleted_emails = await delete_by_list(session, emails_to_delete)
        await session.commit()
        
        # Проверяем результат
        assert len(deleted_emails) == 2
        assert "bulk1@example.com" in deleted_emails
        assert "bulk2@example.com" in deleted_emails
        
        # Проверяем, что bulk3 остался
        remaining_users = await get_all(session)
        assert len(remaining_users) == 1
        assert remaining_users[0].user_email == "bulk3@example.com"
    
    @pytest.mark.asyncio
    async def test_delete_by_list_partial_exist(self, session):
        """Тест удаления когда некоторые email не существуют."""
        # Создаем только одного пользователя
        user_data = {"username": "Only One", "user_email": "only.one@example.com"}
        await user_create(session, user_data)
        await session.commit()
        
        # Пытаемся удалить несколько email, включая несуществующие
        emails_to_delete = ["only.one@example.com", "nonexistent@example.com"]
        deleted_emails = await delete_by_list(session, emails_to_delete)
        await session.commit()
        
        # Должен удалиться только существующий
        assert len(deleted_emails) == 1
        assert "only.one@example.com" in deleted_emails
        
        # Проверяем, что база пуста
        remaining_users = await get_all(session)
        assert len(remaining_users) == 0
    
    @pytest.mark.asyncio
    async def test_delete_by_list_none_exist(self, session):
        """Тест удаления несуществующих email."""
        emails_to_delete = ["fake1@example.com", "fake2@example.com"]
        deleted_emails = await delete_by_list(session, emails_to_delete)
        await session.commit()
        
        assert len(deleted_emails) == 0
    
    @pytest.mark.asyncio
    async def test_delete_by_list_empty(self, session):
        """Тест удаления с пустым списком."""
        deleted_emails = await delete_by_list(session, [])
        await session.commit()
        
        assert len(deleted_emails) == 0


# === Тесты уникальности email ===
class TestEmailUniqueness:
    """Тесты проверки уникальности email."""
    
    @pytest.mark.asyncio
    async def test_duplicate_email_raises_error(self, session):
        """Тест попытки создания пользователя с дублирующимся email."""
        from sqlalchemy.exc import IntegrityError
        
        # Создаем первого пользователя
        user_data = {
            "username": "First User",
            "user_email": "unique@example.com"
        }
        await user_create(session, user_data)
        await session.commit()
        
        # Пытаемся создать второго с тем же email
        duplicate_data = {
            "username": "Second User",
            "user_email": "unique@example.com"
        }
        
        with pytest.raises(IntegrityError):
            await user_create(session, duplicate_data)
            await session.commit()


# === Тесты валидации данных ===
class TestDataValidation:
    """Тесты валидации входных данных."""
    
    @pytest.mark.asyncio
    async def test_create_user_with_empty_username(self, session):
        """Тест создания пользователя с пустым username."""
        user_data = {
            "username": "",
            "user_email": "empty.username@example.com"
        }
        
        # SQLAlchemy может разрешить пустую строку, но не None
        created_user = await user_create(session, user_data)
        await session.commit()
        
        assert created_user.username == ""
    
    @pytest.mark.asyncio
    async def test_create_user_without_required_fields(self, session):
        """Тест создания пользователя без обязательных полей."""
        # Попытка создать без email должна вызвать ошибку
        with pytest.raises((KeyError, TypeError)):
            user_data = {"username": "No Email"}
            await user_create(session, user_data)


# === Интеграционные тесты ===
class TestIntegration:
    """Интеграционные тесты полного цикла."""
    
    @pytest.mark.asyncio
    async def test_full_crud_cycle(self, session):
        """Тест полного цикла: создание, чтение, обновление (через пересоздание), удаление."""
        # CREATE
        user_data = {
            "username": "Full Cycle User",
            "user_email": "fullcycle@example.com"
        }
        created = await user_create(session, user_data)
        await session.commit()
        assert created.user_id is not None
        
        # READ
        retrieved = await user_get(session, created.user_id)
        assert retrieved is not None
        assert retrieved.username == "Full Cycle User"
        
        # UPDATE (имитация через изменение данных)
        # В текущей реализации нет update функции, проверяем что можем получить
        
        # DELETE
        await user_delete(session, created.user_id)
        await session.commit()
        
        # VERIFY DELETION
        after_delete = await user_get(session, created.user_id)
        assert after_delete is None
    
    @pytest.mark.asyncio
    async def test_bulk_operations(self, session):
        """Тест массовых операций."""
        # Создаем 10 пользователей
        for i in range(10):
            user_data = {
                "username": f"Bulk Test {i}",
                "user_email": f"bulk{i}@test.com"
            }
            await user_create(session, user_data)
        await session.commit()
        
        # Проверяем что все созданы
        all_users = await get_all(session)
        assert len(all_users) >= 10
        
        # Удаляем половину
        emails_to_delete = [f"bulk{i}@test.com" for i in range(5)]
        deleted = await delete_by_list(session, emails_to_delete)
        await session.commit()
        
        assert len(deleted) == 5
        
        # Проверяем что осталось 5
        remaining = await get_all(session)
        assert len(remaining) == 5
