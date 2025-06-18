from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
import jwt
import bcrypt
import uvicorn

# === Config ===
SECRET_KEY = "super-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()

# === In-Memory User Store (for demo only) ===
users_db = {
    "admin": {
        "username": "admin",
        "full_name": "Admin User",
        "hashed_password": bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode(),
    }
}


# === Models ===
class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class User(BaseModel):
    username: str
    full_name: str


class Student(BaseModel):
    id: int
    name: str
    age: int
    grade: str


# === Dummy Data ===
students: List[Student] = [
    Student(id=1, name="Alice", age=15, grade="10"),
    Student(id=2, name="Bob", age=16, grade="11"),
]


# === Auth Helpers ===
def authenticate_user(username: str, password: str) -> Optional[User]:
    user = users_db.get(username)
    if not user:
        return None
    if not bcrypt.checkpw(password.encode(), user["hashed_password"].encode()):
        return None
    return User(username=user["username"], full_name=user["full_name"])


def create_token(user: User) -> str:
    payload = {
        "sub": user.username,
        "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(authorization: str = Header(...)) -> User:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    token = authorization[len("Bearer ") :]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        user = users_db.get(username)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return User(username=user["username"], full_name=user["full_name"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# === Routes ===


@app.post("/token", response_model=TokenResponse)
def login(data: LoginRequest):
    user = authenticate_user(data.username, data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = create_token(user)
    return TokenResponse(access_token=token)


@app.get("/me", response_model=User)
def me(current_user: User = Depends(get_current_user)):
    return current_user


@app.get("/students", response_model=List[Student])
def list_students(current_user: User = Depends(get_current_user)):
    return students


# --- Run Server ---
def main():
    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
