import random
from datetime import datetime, timedelta
from typing import List

import bcrypt
import jwt
import uvicorn
from faker import Faker
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

# ------------------ JWT Config ------------------
SECRET_KEY = "your-super-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# ------------------ User Model ------------------
class Token(BaseModel):
    access_token: str
    token_type: str


class User(BaseModel):
    username: str
    full_name: str


class UserInDB(User):
    hashed_password: str


# ------------------ Student Model ------------------
class Student(BaseModel):
    id: int
    name: str
    age: int
    grade: str


# ------------------ In-Memory Data ------------------
fake_users_db = {
    "admin": {
        "username": "admin",
        "full_name": "Admin User",
        "hashed_password": bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode(),
    }
}

faker = Faker()
grades = ["6", "7", "8", "9", "10", "11", "12"]
students_data: List[Student] = [
    Student(
        id=i, name=faker.name(), age=random.randint(11, 18), grade=random.choice(grades)
    )
    for i in range(1, 11)
]


# ------------------ Auth Logic ------------------
def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


def get_user(username: str):
    user = fake_users_db.get(username)
    if user:
        return UserInDB(**user)


def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user or not verify_password(password, user.hashed_password):
        return None
    return user


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = get_user(username)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ------------------ Auth Routes ------------------
@app.post("/token", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    token = create_access_token(data={"sub": user.username})
    return {"access_token": token, "token_type": "bearer"}


# ------------------ Protected Routes ------------------
@app.get("/me", response_model=User)
def read_me(current_user: User = Depends(get_current_user)):
    return current_user


@app.get("/students", response_model=List[Student])
def get_students(current_user: User = Depends(get_current_user)):
    return students_data


@app.post("/students", response_model=Student)
def add_student(student: Student, current_user: User = Depends(get_current_user)):
    if any(s.id == student.id for s in students_data):
        raise HTTPException(status_code=400, detail="Student ID already exists")
    students_data.append(student)
    return student


# --- Run Server ---
def main():
    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
