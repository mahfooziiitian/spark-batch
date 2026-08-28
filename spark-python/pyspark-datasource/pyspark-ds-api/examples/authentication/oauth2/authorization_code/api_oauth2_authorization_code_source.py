import os

import uvicorn
from authlib.integrations.starlette_client import OAuth
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Load environment variables
load_dotenv()

# Setup FastAPI and templates
app = FastAPI()
templates = Jinja2Templates(directory="templates")
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

app = FastAPI()
# we need this to save temporary code & state in session
app.add_middleware(SessionMiddleware, secret_key="some-random-string")

# Configure OAuth with Authlib
oauth = OAuth()

oauth.register(
    name="github",
    client_id=os.getenv("GITHUB_CLIENT_ID"),
    client_secret=os.getenv("GITHUB_CLIENT_SECRET"),
    access_token_url="https://github.com/login/oauth/access_token",
    authorize_url="https://github.com/login/oauth/authorize",
    api_base_url="https://api.github.com/",
    client_kwargs={"scope": "read:user"},
)


# Home page with login link
@app.get("/", response_class=HTMLResponse)
def homepage(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})


# Redirect to GitHub for authentication
@app.get("/login")
async def login(request: Request):
    redirect_uri = request.url_for("auth")
    return await oauth.github.authorize_redirect(request, redirect_uri)


# GitHub callback: exchange code for token and fetch user info
@app.get("/auth/github/callback")
async def auth(request: Request):
    token = await oauth.github.authorize_access_token(request)
    user_resp = await oauth.github.get("user", token=token)
    user_data = user_resp.json()

    return {
        "username": user_data["login"],
        "name": user_data.get("name"),
        "avatar_url": user_data["avatar_url"],
        "profile": user_data["html_url"],
    }


# --- Run Server ---
def main():
    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
