import random
import secrets
from typing import List

import uvicorn
from faker import Faker
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel

# --- Auth setup ---
security = HTTPBasic()
USERNAME = "admin"
PASSWORD = "secret123"


def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, USERNAME)
    correct_password = secrets.compare_digest(credentials.password, PASSWORD)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# --- FastAPI app ---
app = FastAPI()


# --- Pydantic model and fake datasource ---
class Student(BaseModel):
    id: int
    name: str
    age: int
    grade: str


faker = Faker()
grades = ["6", "7", "8", "9", "10", "11", "12"]

# Generate 100 fake students
students_db: List[Student] = [
    Student(
        id=i, name=faker.name(), age=random.randint(11, 18), grade=random.choice(grades)
    )
    for i in range(1, 101)
]


# --- Routes ---
@app.get("/students", response_model=List[Student])
def get_students(username: str = Depends(authenticate)):
    return students_db


@app.get("/")
def root():
    return {"message": "Welcome to the student API. Use /students with Basic Auth."}


# --- Run Server ---
def main():
    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
