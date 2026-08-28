import os
import random
from typing import List

import uvicorn
from faker import Faker
from fastapi import FastAPI, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

# --- Security Setup ---
# Non-secret fixture key for this local-only demo — override both here and
# in api_key_header.yaml via API_KEY_HEADER_DEMO_KEY if you want a different
# value. Deliberately low-entropy/non-secret-looking so it isn't mistaken
# for a real credential.
DEFAULT_DEMO_KEY = "demo-local-fixture-key"
API_KEYS = [os.environ.get("API_KEY_HEADER_DEMO_KEY", DEFAULT_DEMO_KEY)]

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
