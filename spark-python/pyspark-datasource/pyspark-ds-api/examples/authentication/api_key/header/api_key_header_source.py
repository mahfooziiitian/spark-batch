import random
from typing import List

import uvicorn
from faker import Faker
from fastapi import FastAPI, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

# --- Security Setup ---
API_KEYS = [
    "9d207bf0-10f5-4d8f-a479-22ff5aeff8d1",
    "f47d4a2c-24cf-4745-937e-620a5963c0b8",
    "b7061546-75e8-444b-a2c4-f19655d07eb8",
]

api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def get_api_key(api_key_header: str = Security(api_key_header)) -> str:
    if api_key_header in API_KEYS:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key in header",
        headers={"WWW-Authenticate": "API Key"},
    )


# --- FastAPI App & Models ---
app = FastAPI()


class Student(BaseModel):
    id: int
    name: str
    age: int
    grade: str


# Generate 100 fake student records
faker = Faker()
grades = ["6", "7", "8", "9", "10", "11", "12"]

students_data: List[Student] = [
    Student(
        id=i, name=faker.name(), age=random.randint(11, 18), grade=random.choice(grades)
    )
    for i in range(1, 101)
]


# --- Endpoints ---
@app.get("/public")
def public():
    return {"message": "Public Endpoint"}


@app.get("/private")
def private(api_key: str = Security(get_api_key)):
    return {"message": "Private Endpoint", "api_key": api_key}


@app.get("/students", response_model=List[Student])
def get_students(api_key: str = Security(get_api_key)):
    return students_data


# --- Run Server ---
def main():
    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
