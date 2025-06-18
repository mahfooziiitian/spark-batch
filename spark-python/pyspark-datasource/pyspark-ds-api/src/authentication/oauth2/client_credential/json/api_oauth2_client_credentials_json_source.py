from fastapi import FastAPI, Depends, HTTPException, status, Request
from pydantic import BaseModel
from datetime import datetime, timedelta
import jwt
from jwt import PyJWTError
import uvicorn

app = FastAPI()

# Dummy client credentials
fake_clients_db = {
    "client_id_123": {"client_secret": "secret_abc", "scopes": ["read", "write"]}
}

# JWT config
SECRET_KEY = "your_secret_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# Token response model
class Token(BaseModel):
    access_token: str
    token_type: str


# Token request model (JSON body)
class TokenRequest(BaseModel):
    grant_type: str
    client_id: str
    client_secret: str


# Create JWT access token
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# Token endpoint using JSON
@app.post("/token", response_model=Token)
def login_for_access_token(token_request: TokenRequest):
    if token_request.grant_type != "client_credentials":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported grant type",
        )

    client = fake_clients_db.get(token_request.client_id)
    if not client or client["client_secret"] != token_request.client_secret:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid client credentials",
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    token_data = {
        "sub": token_request.client_id,
        "scopes": client["scopes"],
    }
    access_token = create_access_token(
        data=token_data, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


# Manual token verification
def get_current_client(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401, detail="Missing or invalid Authorization header"
        )

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        client_id = payload.get("sub")
        if client_id is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return client_id
    except PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# Protected endpoints
@app.get("/data")
def read_data(client_id: str = Depends(get_current_client)):
    return {"message": f"Access granted to client {client_id}", "data": [1, 2, 3, 4, 5]}


students_db = [
    {"id": 1, "name": "Alice", "major": "Computer Science"},
    {"id": 2, "name": "Bob", "major": "Mathematics"},
    {"id": 3, "name": "Charlie", "major": "Physics"},
]


@app.get("/students")
def get_students(client_id: str = Depends(get_current_client)):
    return {"client": client_id, "students": students_db}


# Run server
def main():
    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
