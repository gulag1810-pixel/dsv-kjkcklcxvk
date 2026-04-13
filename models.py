from core import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import validates
import re


class User(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False)
    user_email = Column(String(255), unique=True, nullable=False) 

    @validates('user_email')
    def validate_email(self, key, address):
        if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", address):
            raise ValueError("Incorrect email")
        return address
    