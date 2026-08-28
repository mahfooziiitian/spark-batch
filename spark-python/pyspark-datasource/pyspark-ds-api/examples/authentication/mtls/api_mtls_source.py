import random
from pathlib import Path
from typing import List

import uvicorn
from faker import Faker
from fastapi import FastAPI
from pydantic import BaseModel

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
@app.get("/students", response_model=List[Student])
def get_students():
    return students_data


# --- Run Server ---
def main():
    current_dir = Path(__file__).parent
    ssl_keyfile = f"{current_dir/'certs/server.key'}"
    ssl_certfile = f"{current_dir/'certs/server.pem'}"
    ssl_ca_certs = f"{current_dir/'certs/ca.pem'}"
    print(f"SSL Key File: {ssl_keyfile}")
    print(f"SSL Cert File: {ssl_certfile}")
    print(f"SSL CA Certs: {ssl_ca_certs}")
    print("Starting server on https://localhost:8443/students")
    print("Use client.pem and client.key for mTLS authentication.")
    print("Use ca.pem to verify the server's certificate.")
    print("Press Ctrl+C to stop the server.")
    uvicorn.run(
        app,
        host="localhost",
        port=8443,
        log_level="info",
        ssl_keyfile=ssl_keyfile,
        ssl_ca_certs=ssl_ca_certs,
        ssl_cert_reqs=2,
        ssl_certfile=ssl_certfile,
    )


if __name__ == "__main__":
    main()
